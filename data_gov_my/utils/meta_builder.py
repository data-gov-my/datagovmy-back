from __future__ import annotations

import importlib
import json
import logging
import os
import traceback
from abc import ABC, abstractmethod
from os.path import isfile, join
from typing import List

import pandas as pd
from django.apps import apps
from django.core.cache import cache
from django.core.exceptions import FieldDoesNotExist
from pydantic import BaseModel

from data_gov_my.explorers import class_list as exp_class
from data_gov_my.models import (
    CatalogJson,
    DashboardJson,
    ExplorersUpdate,
    FormTemplate,
    MetaJson,
    Publication,
    PublicationDocumentation,
    PublicationDocumentationResource,
    PublicationResource,
    PublicationUpcoming,
    i18nJson,
)
from data_gov_my.utils import common, triggers
from data_gov_my.utils.chart_builders import ChartBuilder
from data_gov_my.utils.common import LANGUAGE_CHOICES
from data_gov_my.utils.cron_utils import (
    create_directory,
    extract_zip,
    fetch_from_git,
    get_latest_info_git,
    remove_src_folders,
    revalidate_frontend,
    write_as_binary,
    upload_s3,
)
from data_gov_my.utils.metajson_structures import (
    DashboardValidateModel,
    DataCatalogValidateModel,
    ExplorerValidateModel,
    FormValidateModel,
    PublicationDocumentationValidateModel,
    PublicationUpcomingValidateModel,
    PublicationValidateModel,
    i18nValidateModel,
)

logger = logging.getLogger("django")

MAX_SUCCESSFUL_BUILD_LOGS_OBJECT_LENGTH = 15


class GeneralMetaBuilder(ABC):
    subclasses_by_category = {}
    subclasses_by_github_dir = {}

    def __init_subclass__(cls, **kwargs) -> None:
        """
        Keep a dictionary of concrete children builders based on the CATEGORY and GITHUB_DIR properties
        """
        super().__init_subclass__(**kwargs)
        cls.subclasses_by_category[cls.CATEGORY] = cls
        cls.subclasses_by_github_dir[cls.GITHUB_DIR] = cls

    @property
    @abstractmethod
    def VALIDATOR(self) -> BaseModel:
        """
        Refers to the pydantic model for metajson structure validation.
        """
        pass

    @property
    @abstractmethod
    def CATEGORY(self) -> str:
        """
        Refers to what category the meta builder belongs to, e.g. "DASHBOARDS", "DATA_CATALOG"...
        """
        pass

    @property
    @abstractmethod
    def GITHUB_DIR(self) -> str:
        """
        Refers to what github directory the metajson files reside in within the meta repository.
        """
        pass

    @property
    @abstractmethod
    def MODEL(self):
        """
        Refers to the Django model class that the meta builder builds.
        """
        pass

    @classmethod
    def create(cls, property: str, isCategory: bool = True) -> GeneralMetaBuilder:
        """
        Given the property, returns the corresponding meta builder. If isCategory is False, the property is assumed to be github_dir.
        """
        subclasses = (
            cls.subclasses_by_category if isCategory else cls.subclasses_by_github_dir
        )
        if property not in subclasses:
            raise ValueError(
                f"'{property}' is not a valid {'category' if isCategory else 'github_dir'}."
            )

        return subclasses[property]()

    @classmethod
    def selective_update(cls):
        """
        Selectively update database objects based on the latest commit in the github repository.
        """
        latest_sha = get_latest_info_git("SHA", "")
        data = json.loads(get_latest_info_git("COMMIT", latest_sha))
        changed_files = [f["filename"] for f in data["files"]]
        refreshed = GeneralMetaBuilder.refresh_meta_repo()

        if not refreshed:
            logging.warning("Github repo has not been refreshed, abort building")
        else:
            filtered_changes = cls.filter_changed_files(changed_files)

            for dir, files in filtered_changes.items():
                if files:
                    builder = GeneralMetaBuilder.create(dir, isCategory=False)
                    builder.build_operation(
                        manual=False, rebuild=False, meta_files=files, refresh=False
                    )

    @staticmethod
    def filter_changed_files(file_list) -> dict[str, list]:
        """
        Maps the files to respective categories and returns a dictionary where the keys are the categories and the values are the corresponding files.
        """
        changed_files = {
            category: [] for category in GeneralMetaBuilder.subclasses_by_github_dir
        }
        meta_dir = os.path.join("DATAGOVMY_SRC", os.getenv("GITHUB_DIR", "-"))
        # clone meta repo if needed
        if not os.path.exists(meta_dir):
            GeneralMetaBuilder.refresh_meta_repo()

        for f in file_list:
            f_info = f.split("/")
            for github_dir in changed_files:
                github_dir_info = github_dir.split("/")
                if f_info[: len(github_dir_info)] == github_dir_info:
                    dir = os.path.join(*f_info[len(github_dir_info) :]).replace(
                        ".json", ""
                    )
                    changed_files[github_dir].append(dir)
        return changed_files

    @staticmethod
    def refresh_meta_repo():
        """
        Delete local github folder (if any), and clone the whole github repo content locally.
        """
        # 1. Wipe github
        remove_src_folders()
        dir_name = "DATAGOVMY_SRC"
        zip_name = "repo.zip"
        git_url = os.getenv("GITHUB_URL", "-")
        # git_token = os.getenv("GITHUB_TOKEN", "-")

        # 2. clone github
        create_directory(dir_name)
        res = fetch_from_git(zip_name, git_url)
        if res.get("resp_code", 400) == 200:
            write_as_binary(res["file_name"], res["data"])
            extract_zip(res["file_name"], dir_name)
            return True

        return False

    def get_github_directory(self):
        """
        Returns the local github directory based on defined category.
        """
        return os.path.join(
            os.getcwd(),
            "DATAGOVMY_SRC",
            os.getenv("GITHUB_DIR", "-"),
            self.GITHUB_DIR,
        )

    def get_meta_files(self):
        """
        Returns all the meta json files within the github directory.
        """
        meta_dir = self.get_github_directory()
        return [f for f in os.listdir(meta_dir) if isfile(os.path.join(meta_dir, f))]

    def additional_handling(self, rebuild: bool, meta_files, created_objects):
        return created_objects

    @abstractmethod
    def update_or_create_meta(self, filename: str, metadata: dict):
        """
        Updates or creates the database model object based on the metadata taken from github.
        """
        pass

    def revalidate_route(self, objects: List):
        """
        Only applicable if objects (model instances) has "route" field, else will not be called.
        """
        if len(objects) < 1:
            return

        triggers.send_telegram(
            "\n".join(
                [
                    triggers.format_header(
                        f"PERFORMING REVALIDATION ({self.MODEL.__name__})"
                    ),
                    triggers.format_files_with_status_emoji(objects, "🔄"),
                ]
            )
        )

        for model_obj in objects:
            routes = model_obj.route
            sites = model_obj.sites
            if not routes and not sites:  # current object does not have any routes
                continue

            for site in sites:
                successful_routes = []
                failed_routes = []
                failed_info = []
                telegram_msg = (
                    triggers.format_header(
                        f"<code>{str(model_obj).upper()}</code> REVALIDATION STATUS @ <b>{site}</b>"
                    )
                    + "\n"
                )
                if routes:
                    response = revalidate_frontend(routes=routes, site=site)
                    if response.status_code == 200:
                        successful_routes.extend(response.json()["revalidated"])
                    elif response.status_code == 400:
                        failed_routes.extend(routes.split(","))
                        failed_info.append(response.json())
                    else:
                        failed_routes.extend(routes.split(","))
                        failed_info.append(
                            {"DB OBJECT": str(model_obj), "ERROR": "Unknown :("}
                        )

                if len(successful_routes) >= MAX_SUCCESSFUL_BUILD_LOGS_OBJECT_LENGTH:
                    successful_log = f"✅︎ <b>{len(successful_routes)}</b> routes have been successfully revalidated!\n"
                else:
                    successful_log = triggers.format_files_with_status_emoji(
                        successful_routes, "✅︎"
                    )
                telegram_msg += successful_log
                telegram_msg += "\n\n"
                telegram_msg += triggers.format_files_with_status_emoji(
                    failed_routes, "❌"
                )

                if len(failed_info) > 0:
                    telegram_msg += "\n\n"
                    telegram_msg += triggers.format_multi_line(
                        failed_info, "FAILED REVALIDATION INFO"
                    )

                triggers.send_telegram(telegram_msg)

    def remove_deleted_files(self):
        """
        Removes the deleted files in the repo, from the db
        """
        for i in ["dashboards", "catalog"]:
            _DIR = os.path.join(
                os.getcwd(), "DATAGOVMY_SRC", os.getenv("GITHUB_DIR", "-"), i
            )

            column = "dashboard_name" if i == "dashboards" else "file_src"
            model_name = "DashboardJson" if i == "dashboards" else "CatalogJson"
            model = apps.get_model("data_gov_my", model_name)

            distinct_db = [
                m[column] for m in model.objects.values(column).distinct()
            ]  # Change model here
            distinct_dir = [
                f.replace(".json", "")
                for f in os.listdir(_DIR)
                if isfile(join(_DIR, f))
            ]
            diff = list(set(distinct_db) - set(distinct_dir))
            if diff:
                query = {f"{column}__in": diff}
                if i == "dashboards":
                    DashboardJson.objects.filter(**query).delete()
                    MetaJson.objects.filter(**query).delete()
                else:
                    CatalogJson.objects.filter(**query).delete()

    def build_operation(self, manual=True, rebuild=True, meta_files=[], refresh=True):
        """
        General build operation for all data builder classes.
        Inherited classes should override `update_or_create_meta()` to control how each meta file is used to update or create model objects.
        Steps taken:
        1. Refresh github repository (delete and re-clone)
        2. Collect meta files (if no meta files provided in input, the whole folder will be taken)
        3. Calls `update_or_create_meta()` to save metadata into database as model instances.
        4. Calls `additional_handling()`, e.g. each dashboard metadata has multiple charts, these charts are individually updated through `additional_handling()`.
        5. Revalidates routes if the model instances have `route` field.
        """
        if refresh:
            self.refresh_meta_repo()
        # Remove from db, deleted meta jsons
        self.remove_deleted_files()  # FIXME: refactor to delete from *all* categories?

        # get meta files (prioritise input files)
        meta_files = (
            [f + ".json" for f in meta_files] if meta_files else self.get_meta_files()
        )

        # send telegram message
        operation_type = "REBUILD" if rebuild else "UPDATE"
        trigger_type = "MANUAL" if manual else "SELECTIVE"
        operation_files = triggers.format_files_with_status_emoji(meta_files, "🔄")
        triggers.send_telegram(
            triggers.format_header(
                f"PERFORMING {self.CATEGORY} {operation_type} ({trigger_type})"
            )
            + "\n"
            + operation_files
        )

        if rebuild:
            self.MODEL.objects.all().delete()

        failed = []
        meta_objects = []
        for meta in meta_files:
            try:
                f_meta = os.path.join(self.get_github_directory(), meta)
                f = open(f_meta)
                data = json.load(f)
                validated_metadata = self.VALIDATOR.model_validate(data)
                created_object = self.update_or_create_meta(meta, validated_metadata)
                if isinstance(created_object, list):
                    for object in created_object:
                        object.save()
                    meta_objects.extend(created_object)
                else:
                    created_object.save()
                    meta_objects.append(created_object)
                f.close()
            except Exception as e:
                logger.error(traceback.format_exc())
                failed.append({"FILE": meta, "ERROR": e})

        if len(meta_objects) >= MAX_SUCCESSFUL_BUILD_LOGS_OBJECT_LENGTH:
            successful_log = (
                f"✅︎ <b>{len(meta_objects)}</b> objects have been successfully built!\n"
            )
        else:
            successful_log = triggers.format_files_with_status_emoji(meta_objects, "✅︎")
        telegram_msg = [
            triggers.format_header(f"Meta Built Status ({self.MODEL.__name__})"),
            successful_log + "\n",
            triggers.format_files_with_status_emoji(
                [obj["FILE"] for obj in failed], "❌"
            ),
        ]

        if failed:
            telegram_msg.append(
                "\n" + triggers.format_multi_line(failed, "Failed Meta - Error logs")
            )

        triggers.send_telegram("\n".join(telegram_msg))

        meta_objects = self.additional_handling(rebuild, meta_files, meta_objects)

        if self.model_has_field("route"):
            self.revalidate_route(meta_objects)

    def model_has_field(self, field: str) -> bool:
        """
        Returns True if model has the input field, else False.
        """
        try:
            field = self.MODEL._meta.get_field(field)
            return True
        except FieldDoesNotExist:
            return False


class DashboardBuilder(GeneralMetaBuilder):
    CATEGORY = "DASHBOARDS"
    MODEL = MetaJson
    GITHUB_DIR = "dashboards"
    VALIDATOR = DashboardValidateModel

    def update_or_create_meta(self, filename: str, metadata: DashboardValidateModel):
        dashboard_meta = metadata.model_dump()
        updated_values = {
            "dashboard_meta": dashboard_meta,
            "route": metadata.route,
            "sites": metadata.sites,
        }
        obj, created = MetaJson.objects.update_or_create(
            dashboard_name=metadata.dashboard_name,
            defaults=updated_values,
        )

        cache.set("META_" + metadata.dashboard_name, dashboard_meta)
        return obj

    def additional_handling(
        self, rebuild: bool, meta_files, created_objects: List[MetaJson]
    ):
        """
        Update or create new DashboardJson instances (unique chart data) based on each created MetaJson instance.
        """
        if rebuild:
            DashboardJson.objects.all().delete()

        successful_meta = set()

        for meta in created_objects:
            failed = []
            created_charts = []
            dbd_meta: dict = meta.dashboard_meta
            dbd_name = meta.dashboard_name
            chart_list = dbd_meta["charts"]

            for k in chart_list.keys():
                chart_name = k
                chart_type = chart_list[k]["chart_type"]
                c_data = {}
                c_data["variables"] = chart_list[k]["variables"]
                c_data["input"] = chart_list[k]["chart_source"]
                api_type = chart_list[k]["api_type"]
                try:
                    res = {}
                    builder = ChartBuilder.create(chart_type)
                    chart_data = builder.build_chart(
                        c_data["input"], c_data["variables"]
                    )
                    res["data"] = chart_data
                    if len(res["data"]) > 0:  # If the dict isnt empty
                        if "data_as_of" in chart_list[k]:
                            res["data_as_of"] = chart_list[k]["data_as_of"]

                        updated_values = {
                            "chart_type": chart_type,
                            "api_type": api_type,
                            "chart_data": res,
                        }
                        obj, created = DashboardJson.objects.update_or_create(
                            dashboard_name=dbd_name,
                            chart_name=k,
                            defaults=updated_values,
                        )
                        obj.save()
                        created_charts.append(obj)
                        successful_meta.add(meta)
                        cache.set(dbd_name + "_" + k, res)

                except Exception as e:
                    failed_obj = {}
                    failed_obj["DASHBOARD"] = dbd_name
                    failed_obj["CHART_NAME"] = chart_name
                    failed_obj["ERROR"] = str(e)
                    logger.error(traceback.format_exc())
                    failed.append(failed_obj)

            # For a single dashboard, send status on all its charts
            telegram_msg = [
                triggers.format_header(
                    f"<code>{dbd_name.upper()}</code> Charts Built Status (DashboardJson)"
                ),
                triggers.format_files_with_status_emoji(created_charts, "✅︎") + "\n",
                triggers.format_files_with_status_emoji(
                    [f'{obj["DASHBOARD"]} ({obj["CHART_NAME"]})' for obj in failed],
                    "❌",
                ),
            ]

            if failed:
                telegram_msg.append(
                    "\n"
                    + triggers.format_multi_line(failed, "Failed Meta - Error logs")
                )

            triggers.send_telegram("\n".join(telegram_msg))

        return successful_meta


class i18nBuilder(GeneralMetaBuilder):
    CATEGORY = "I18N"
    MODEL = i18nJson
    GITHUB_DIR = "i18n"
    VALIDATOR = i18nValidateModel

    def get_meta_files(self):
        """
        Returns all meta files under each language sub-folder.
        """
        meta_dir = self.get_github_directory()
        files = []
        for lang_code, lang_name in LANGUAGE_CHOICES:
            lang_dir = os.path.join(meta_dir, lang_code)
            files.extend(
                [
                    os.path.join(lang_code, f)
                    for f in os.listdir(lang_dir)
                    if isfile(os.path.join(lang_dir, f))
                ]
            )
        return files

    def update_or_create_meta(self, filename: str, metadata: i18nValidateModel):
        language, filename = os.path.split(filename)
        filename = filename.replace(".json", "")

        env_location = os.getenv("GITHUB_SHA_URL")
        s3_key = f"{env_location}/{language}/{filename}.json"
        res = upload_s3(
            data=metadata.translation, bucket=os.getenv("S3_I18N_BUCKET"), key=s3_key
        )

        if res:
            obj, created = i18nJson.objects.update_or_create(
                filename=filename, language=language, defaults=metadata.model_dump()
            )
            return obj


class FormBuilder(GeneralMetaBuilder):
    CATEGORY = "FORMS"
    MODEL = FormTemplate
    GITHUB_DIR = "forms"
    VALIDATOR = FormValidateModel

    def update_or_create_meta(self, filename: str, metadata: FormValidateModel):
        form_type = filename.replace(".json", "")
        return FormTemplate.create(form_type=form_type, form_meta=metadata.model_dump())


class DataCatalogBuilder(GeneralMetaBuilder):
    CATEGORY = "DATA_CATALOG"
    MODEL = CatalogJson
    GITHUB_DIR = "catalog"
    VALIDATOR = DataCatalogValidateModel

    def update_or_create_meta(self, filename: str, metadata: DataCatalogValidateModel):
        file_data = metadata.file
        all_variable_data = metadata.file.variables
        full_meta = metadata
        file_src = filename.replace(".json", "")

        created_objects = []

        for cur_data in all_variable_data:
            if "catalog_data" in cur_data:  # Checks if the catalog_data is in
                cur_catalog_data = cur_data["catalog_data"]
                chart_type = cur_catalog_data["chart"]["chart_type"]

                if chart_type in common.CHART_TYPES:
                    args = {
                        "full_meta": full_meta.model_dump(),
                        "file_data": file_data.model_dump(),
                        "cur_data": cur_data,
                        "all_variable_data": all_variable_data,
                        "file_src": file_src,
                    }

                    module_ = f"data_gov_my.catalog_utils.catalog_variable_classes.{common.CHART_TYPES[chart_type]['parent']}"
                    constructor_ = common.CHART_TYPES[chart_type]["constructor"]
                    class_ = getattr(importlib.import_module(module_), constructor_)
                    obj = class_(**args)

                    unique_id = obj.unique_id
                    db_input = obj.db_input
                    db_input["exclude_openapi"] = file_data.exclude_openapi
                    db_obj, created = CatalogJson.objects.update_or_create(
                        id=unique_id, defaults=db_input
                    )

                    created_objects.append(db_obj)

        return created_objects


class ExplorerBuilder(GeneralMetaBuilder):
    CATEGORY = "EXPLORERS"
    MODEL = ExplorersUpdate
    GITHUB_DIR = "explorers"
    VALIDATOR = ExplorerValidateModel

    def update_or_create_meta(self, filename: str, metadata: ExplorerValidateModel):
        updated_values = {
            "dashboard_meta": metadata.model_dump(),
            "route": metadata.route,
            "sites": metadata.sites,
        }
        obj, created = MetaJson.objects.update_or_create(
            dashboard_name=metadata.explorer_name,
            defaults=updated_values,
        )

        cache.set("META_" + metadata.explorer_name, metadata)
        return obj

    def additional_handling(
        self, rebuild: bool, meta_files, created_objects: List[MetaJson]
    ):
        """
        Update or create new ExplorersUpdate instances (Entire Table) based on each created MetaJson instance.
        """
        if rebuild:
            ExplorersUpdate.objects.all().delete()

        successful_meta = set()

        for meta in created_objects:
            failed = []
            tables_updated = []
            exp_meta: dict = meta.dashboard_meta
            exp_name = meta.dashboard_name
            table_list = exp_meta["tables"]
            for k in table_list.keys():
                table_name = k
                table_operation = exp_meta["tables"][k]["update"]
                last_update = exp_meta["tables"][k]["data_as_of"]
                try:
                    obj = exp_class.EXPLORERS_CLASS_LIST[exp_meta["explorer_name"]]()

                    upd, create = ExplorersUpdate.objects.update_or_create(
                        explorer=exp_name,
                        file_name=k,
                        defaults={"last_update": last_update},
                    )

                    if table_operation == "SLEEP":
                        continue
                    elif table_operation == "REBUILD":
                        obj.populate_db(table=table_name, rebuild=True)
                    elif table_operation == "UPDATE":
                        unique_keys = exp_meta["tables"][k]["unique_keys"]
                        obj.update(table_name=table_name, unique_keys=unique_keys)

                    successful_meta.add(meta)
                    tables_updated.append(table_name)
                except Exception as e:
                    failed_obj = {}
                    failed_obj["DASHBOARD"] = exp_name
                    failed_obj["CHART_NAME"] = table_name
                    failed_obj["ERROR"] = str(e)
                    logger.error(traceback.format_exc())
                    failed.append(failed_obj)

            # For a single dashboard, send status on all its charts
            telegram_msg = [
                triggers.format_header(
                    f"<code>{exp_name.upper()}</code> Explorer Table Built Status"
                ),
                triggers.format_files_with_status_emoji(tables_updated, "✅︎") + "\n",
                triggers.format_files_with_status_emoji(
                    [f'{obj["DASHBOARD"]} ({obj["CHART_NAME"]})' for obj in failed],
                    "❌",
                ),
            ]

            if failed:
                telegram_msg.append(
                    "\n"
                    + triggers.format_multi_line(failed, "Failed Meta - Error logs")
                )

            triggers.send_telegram("\n".join(telegram_msg))

        return successful_meta


class PublicationBuilder(GeneralMetaBuilder):
    CATEGORY = "PUBLICATION"
    MODEL = Publication
    GITHUB_DIR = "pub-dosm/publications"
    VALIDATOR = PublicationValidateModel

    def update_or_create_meta(self, filename: str, metadata: PublicationValidateModel):
        # english publication
        pub_object_en, _ = Publication.objects.update_or_create(
            publication_id=metadata.publication,
            language="en-GB",
            defaults={
                "publication_type": metadata.publication_type,
                "publication_type_title": metadata.en.publication_type_title,
                "title": metadata.en.title,
                "description": metadata.en.description,
                "release_date": metadata.release_date,
                "frequency": metadata.frequency,
                "geography": metadata.geography,
                "demography": metadata.demography,
            },
        )

        PublicationResource.objects.filter(publication=pub_object_en).delete()
        resources_en = PublicationResource.objects.bulk_create(
            [
                PublicationResource(
                    resource_id=resource.resource_id,
                    resource_type=resource.resource_type,
                    resource_name=resource.resource_name,
                    resource_link=resource.resource_link,
                    publication=pub_object_en,
                )
                for resource in metadata.en.resources
            ]
        )

        # bm publications
        pub_object_bm, _ = Publication.objects.update_or_create(
            publication_id=metadata.publication,
            language="ms-MY",
            defaults={
                "publication_type": metadata.publication_type,
                "publication_type_title": metadata.bm.publication_type_title,
                "title": metadata.bm.title,
                "description": metadata.bm.description,
                "release_date": metadata.release_date,
                "frequency": metadata.frequency,
                "geography": metadata.geography,
                "demography": metadata.demography,
            },
        )

        PublicationResource.objects.filter(publication=pub_object_bm).delete()
        resources_bm = PublicationResource.objects.bulk_create(
            [
                PublicationResource(
                    resource_id=resource.resource_id,
                    resource_type=resource.resource_type,
                    resource_name=resource.resource_name,
                    resource_link=resource.resource_link,
                    publication=pub_object_bm,
                )
                for resource in metadata.bm.resources
            ]
        )

        return [pub_object_en, pub_object_bm]


class PublicationDocumentationBuilder(GeneralMetaBuilder):
    CATEGORY = "PUBLICATION_DOCS"
    MODEL = PublicationDocumentation
    GITHUB_DIR = "pub-dosm/documentation"
    VALIDATOR = PublicationDocumentationValidateModel

    def update_or_create_meta(
        self, filename: str, metadata: PublicationDocumentationValidateModel
    ):
        # english publications
        pub_object_en, _ = PublicationDocumentation.objects.update_or_create(
            publication_id=metadata.publication,
            language="en-GB",
            defaults={
                "documentation_type": metadata.documentation_type,
                "publication_type": metadata.publication_type,
                "publication_type_title": metadata.en.publication_type_title,
                "title": metadata.en.title,
                "description": metadata.en.description,
                "release_date": metadata.release_date,
            },
        )

        PublicationDocumentationResource.objects.filter(
            publication=pub_object_en
        ).delete()
        resources_en = PublicationDocumentationResource.objects.bulk_create(
            [
                PublicationDocumentationResource(
                    resource_id=resource.resource_id,
                    resource_type=resource.resource_type,
                    resource_name=resource.resource_name,
                    resource_link=resource.resource_link,
                    publication=pub_object_en,
                )
                for resource in metadata.en.resources
            ]
        )

        # bm publications
        pub_object_bm, _ = PublicationDocumentation.objects.update_or_create(
            publication_id=metadata.publication,
            language="ms-MY",
            defaults={
                "documentation_type": metadata.documentation_type,
                "publication_type": metadata.publication_type,
                "publication_type_title": metadata.bm.publication_type_title,
                "title": metadata.bm.title,
                "description": metadata.bm.description,
                "release_date": metadata.release_date,
            },
        )

        PublicationDocumentationResource.objects.filter(
            publication=pub_object_bm
        ).delete()
        resources_bm = PublicationDocumentationResource.objects.bulk_create(
            [
                PublicationDocumentationResource(
                    resource_id=resource.resource_id,
                    resource_type=resource.resource_type,
                    resource_name=resource.resource_name,
                    resource_link=resource.resource_link,
                    publication=pub_object_bm,
                )
                for resource in metadata.bm.resources
            ]
        )

        return [pub_object_en, pub_object_bm]


class PublicationUpcomingBuilder(GeneralMetaBuilder):
    CATEGORY = "PUBLICATION_UPCOMING"
    MODEL = PublicationUpcoming
    GITHUB_DIR = "pub-dosm/upcoming"
    VALIDATOR = PublicationUpcomingValidateModel

    def update_or_create_meta(
        self, filename: str, metadata: PublicationUpcomingValidateModel
    ):
        df = pd.read_parquet(metadata.parquet_link)

        PublicationUpcoming.objects.all().delete()

        # english publications
        df_en = (
            df[
                [
                    "publication_id",
                    "publication_type",
                    "release_date",
                    "title_en",
                    "publication_type_en",
                    "product_type_en",
                    "release_series_en",
                ]
            ]
            .copy()
            .rename(
                columns={
                    "title_en": "publication_title",
                    "publication_type_en": "publication_type_title",
                    "product_type_en": "product_type",
                    "release_series_en": "release_series",
                },
                errors="raise",
            )
        )

        publications_en = [
            PublicationUpcoming(**kwargs, language="en-GB")
            for kwargs in df_en.to_dict(orient="records")
        ]

        publications_en_created = PublicationUpcoming.objects.bulk_create(
            publications_en, batch_size=1000
        )

        # bm publications
        df_bm = (
            df[
                [
                    "publication_id",
                    "publication_type",
                    "release_date",
                    "title_bm",
                    "publication_type_bm",
                    "product_type_bm",
                    "release_series_bm",
                ]
            ]
            .copy()
            .rename(
                columns={
                    "title_bm": "publication_title",
                    "publication_type_bm": "publication_type_title",
                    "product_type_bm": "product_type",
                    "release_series_bm": "release_series",
                },
                errors="raise",
            )
        )

        publications_bm = [
            PublicationUpcoming(**kwargs, language="ms-MY")
            for kwargs in df_bm.to_dict(orient="records")
        ]

        publications_bm_created = PublicationUpcoming.objects.bulk_create(
            publications_bm, batch_size=1000
        )

        return publications_en_created + publications_bm_created
