from __future__ import annotations

import json
import logging
import os
import traceback
from abc import ABC, abstractmethod
from datetime import date
from os.path import isfile
from pathlib import Path
from typing import List
from urllib.request import urlopen

import numpy as np
import pandas as pd
from django.core.cache import cache
from django.core.exceptions import FieldDoesNotExist
from post_office import mail
from pydantic import BaseModel
from slugify import slugify

from data_catalogue.metajson_structure import DataCatalogueValidateModel
from data_catalogue.models import (
    DataCatalogue,
    DataCatalogueMeta,
    Dataviz,
    Field,
    RelatedDataset,
    SiteCategory,
)
from data_catalogue.utils import translation
from data_gov_my.explorers import class_list as exp_class
from data_gov_my.models import (
    DashboardJson,
    ExplorersMetaJson,
    ExplorersUpdate,
    FormTemplate,
    MetaJson,
    Publication,
    PublicationDocumentation,
    PublicationDocumentationResource,
    PublicationResource,
    PublicationUpcoming,
    i18nJson,
    Subscription, PublicationType, PublicationSubtype
)
from data_gov_my.utils import triggers
from data_gov_my.utils.chart_builders import ChartBuilder
from data_gov_my.utils.common import LANGUAGE_CHOICES
from data_gov_my.utils.cron_utils import (
    create_directory,
    extract_zip,
    fetch_from_git,
    get_latest_info_git,
    remove_src_folders,
    revalidate_frontend,
    upload_s3,
    write_as_binary,
)
from data_gov_my.utils.metajson_structures import (
    DashboardValidateModel,
    ExplorerValidateModel,
    FormValidateModel,
    PublicationDocumentationValidateModel,
    PublicationUpcomingValidateModel,
    PublicationValidateModel,
    i18nValidateModel, PublicationTypeValidateModel,
)
from data_gov_my.utils.publication_helpers import craft_title, craft_template_en
from data_gov_my.utils.subscription_email_helper import SubscriptionEmail

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
        changed_files = []
        delete_files = []

        for f in data["files"]:
            if f["status"] == "removed":
                delete_files.append(f)
            else:
                changed_files.append(f)
        refreshed = GeneralMetaBuilder.refresh_meta_repo()

        if not refreshed:
            logger.warning("Github repo has not been refreshed, abort building")
        else:
            # build operation (for newly created or modified files)
            filtered_changes = cls.filter_changed_files(
                changed_files, compare_github=True
            )
            for dir, files in filtered_changes.items():
                if files:
                    builder = GeneralMetaBuilder.create(dir, isCategory=False)
                    builder.build_operation(
                        manual=False, rebuild=False, meta_files=files, refresh=False
                    )

            # delete operation (for any files with "removed" as status)
            deletes = cls.filter_changed_files(delete_files)
            for dir, files in deletes.items():
                if files:
                    builder = GeneralMetaBuilder.create(dir, isCategory=False)
                    builder.delete_operation(files)

    @staticmethod
    def filter_changed_files(file_list, compare_github=False) -> dict[str, list[dict]]:
        changed_files = {
            category: [] for category in GeneralMetaBuilder.subclasses_by_github_dir
        }
        meta_dir = os.path.join("DATAGOVMY_SRC", os.getenv("GITHUB_DIR", "-"))
        # clone meta repo if needed
        if not os.path.exists(meta_dir):
            GeneralMetaBuilder.refresh_meta_repo()

        for file in file_list:
            f = file["filename"]
            f_info = f.split("/")
            if f_info[:2] == ["data-catalogue", "openapi"]:
                continue
            for github_dir in changed_files:
                github_dir_info = github_dir.split("/")
                if f_info[: len(github_dir_info)] == github_dir_info:
                    dir = os.path.join(*f_info[len(github_dir_info):]).replace(
                        ".json", ""
                    )
                    if compare_github:  # used for filtering modified/created files
                        changed_files[github_dir].append(dir)
                    else:  # used for deleted files
                        changed_files[github_dir].append(file)
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

    @abstractmethod
    def delete_file(self, filename: str, data: dict):
        # override to handle delete logic
        pass

    def delete_operation(self, meta_files=[]):
        deleted = []
        for file in meta_files:
            # read the deleted content
            deleted_json = urlopen(file["raw_url"])
            deleted_content = json.loads(deleted_json.read())
            deleted_count, deleted_items = self.delete_file(
                file["filename"], deleted_content
            )
            deleted.append(f"{file.get('filename')}\n{deleted_items}")

        triggers.send_telegram(
            triggers.format_header(f"DELETED {self.CATEGORY} REMOVED FILES (SELECTIVE)")
            + "\n"
            + triggers.format_files_with_status_emoji(deleted, "🗑️")
        )

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

    def delete_file(self, filename: str, data: dict):
        meta_count, meta_deleted = MetaJson.objects.filter(
            dashboard_name=data.get("dashboard_name")
        ).delete()
        dashboard_count, dashboard_deleted = DashboardJson.objects.filter(
            dashboard_name=data.get("dashboard_name")
        ).delete()
        meta_deleted.update(dashboard_deleted)
        return meta_count + dashboard_count, meta_deleted

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

    def delete_file(self, filename: str, data: dict):
        logger.warning(f"Please handle deleting {filename} from S3")
        return 0, {}

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

        env_location = os.getenv("ENV_LOCATION").lower()
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

    def delete_file(self, filename: str, data: dict):
        form_type = Path(filename).stem
        return FormTemplate.objects.filter(form_type=form_type).delete()

    def update_or_create_meta(self, filename: str, metadata: FormValidateModel):
        form_type = filename.replace(".json", "")
        return FormTemplate.create(form_type=form_type, form_meta=metadata.model_dump())


class DataCatalogueBuilder(GeneralMetaBuilder):
    CATEGORY = "DATA_CATALOGUE"
    MODEL = DataCatalogueMeta
    GITHUB_DIR = "data-catalogue"
    VALIDATOR = DataCatalogueValidateModel

    default_translation_mapping = {
        "state": translation.STATE_TRANSLATIONS,
        "parlimen": translation.PARLIMEN_TRANSLATIONS,
        "district": translation.DISTRICT_TRANSLATIONS,
        "dun": translation.DUN_TRANSLATIONS,
        "range": translation.RANGE_TRANSLATIONS,
    }

    def delete_file(self, filename: str, data: dict):
        filename = Path(filename).stem
        return DataCatalogueMeta.objects.filter(id=filename).delete()

    def update_or_create_meta(
            self, filename: str, metadata: DataCatalogueValidateModel
    ):
        # check if need to add default translations
        default_keys = set(self.default_translation_mapping)
        filter_keys = set()
        translations_en = metadata.translations_en
        translations_ms = metadata.translations_ms

        for dv in metadata.dataviz:
            filter_keys.update(dv.config.get("filter_columns", []))
        for key in filter_keys.intersection(default_keys):
            translations_en.update(self.default_translation_mapping[key])
            translations_ms.update(self.default_translation_mapping[key])

        dc_meta, is_dc_meta_created = DataCatalogueMeta.objects.update_or_create(
            id=Path(filename).stem,
            defaults=dict(
                exclude_openapi=metadata.exclude_openapi,
                manual_trigger=metadata.manual_trigger,
                title_en=metadata.title_en,
                title_ms=metadata.title_ms,
                title_sort=metadata.title_sort,
                description_en=metadata.description_en,
                description_ms=metadata.description_ms,
                data_as_of=metadata.data_as_of,
                last_updated=metadata.last_updated,
                next_update=metadata.next_update,
                methodology_en=metadata.methodology_en,
                methodology_ms=metadata.methodology_ms,
                caveat_en=metadata.caveat_en,
                caveat_ms=metadata.caveat_ms,
                publication_en=metadata.publication_en,
                publication_ms=metadata.publication_ms,
                link_parquet=metadata.link_parquet,
                link_csv=metadata.link_csv,
                link_preview=metadata.link_preview,
                link_editions=metadata.link_editions,
                frequency=metadata.frequency,
                geography=metadata.geography,
                demography=metadata.demography,
                dataset_begin=metadata.dataset_begin,
                dataset_end=metadata.dataset_end,
                data_source=metadata.data_source,
                translations_en=metadata.translations_en,
                translations_ms=metadata.translations_ms,
            ),
        )

        # Avoid bulk_create as PK is not included in returned list (affects many-to-many .set())
        fields = [
            Field.objects.get_or_create(index=i, **field_data.model_dump())[0]
            for i, field_data in enumerate(metadata.fields)
        ]

        site_categories = [
            SiteCategory.objects.update_or_create(
                site=sc.site,
                category_en=sc.category_en,
                category_ms=sc.category_ms,
                subcategory_en=sc.subcategory_en,
                subcategory_ms=sc.subcategory_ms,
                defaults=dict(
                    category_sort=sc.category_sort, subcategory_sort=sc.subcategory_sort
                ),
            )[0]
            for sc in metadata.site_category
        ]

        related_datasets = RelatedDataset.objects.bulk_create(
            [
                RelatedDataset(**dataset.model_dump())
                for dataset in metadata.related_datasets
            ],
            update_conflicts=True,
            update_fields=[
                # "title",
                # "description",
                "title_en",
                "title_ms",
                "description_en",
                "description_ms",
            ],
            unique_fields=["id"],
        )

        dataviz_objects = [
            Dataviz(catalogue_meta=dc_meta, **dv.model_dump())
            for dv in metadata.dataviz
        ]
        dc_meta.dataviz_set.all().delete()
        Dataviz.objects.bulk_create(dataviz_objects)

        # handle many-to-many fields separately.
        dc_meta.related_datasets.set(related_datasets)
        dc_meta.site_category.set(site_categories)
        dc_meta.fields.set(fields)

        # populate the table data
        parquet_link = metadata.link_preview or metadata.link_parquet
        df = pd.read_parquet(str(parquet_link))
        if "date" in df.columns:
            df["date"] = df["date"].astype(str)

        df = df.replace({np.nan: None})
        data = df.to_dict(orient="records")

        # take all slug fields
        dataviz = metadata.dataviz
        slug_fields = set()
        for dv in dataviz:
            filter_columns = dv.config.get("filter_columns", [])
            slug_fields.update(filter_columns)

        slug_df = df[list(slug_fields)].copy()
        for col in slug_df.columns:
            slug_df[col] = slug_df[col].apply(slugify)

        if slug_fields:
            slug_data = slug_df.to_dict("records")
            catalogue_data = [
                DataCatalogue(
                    index=i, catalogue_meta=dc_meta, data=row, slug=slug_data[i]
                )
                for i, row in enumerate(data)
            ]
        else:
            catalogue_data = [
                DataCatalogue(index=i, catalogue_meta=dc_meta, data=row)
                for i, row in enumerate(data)
            ]

        # side quest: handle the dataviz "dropdown" building
        for dv in dataviz:
            filter_columns = dv.config.get("filter_columns", [])
            dropdown: pd.DataFrame = (
                slug_df[filter_columns].drop_duplicates().to_dict("records")
            )
            Dataviz.objects.filter(
                catalogue_meta=dc_meta, dataviz_id=dv.dataviz_id
            ).update(dropdown=dropdown)

        _, deleted_dc_data = DataCatalogue.objects.filter(
            catalogue_meta=dc_meta
        ).delete()
        DataCatalogue.objects.bulk_create(catalogue_data)

        return dc_meta


class ExplorerBuilder(GeneralMetaBuilder):
    CATEGORY = "EXPLORERS"
    MODEL = ExplorersUpdate
    GITHUB_DIR = "explorers"
    VALIDATOR = ExplorerValidateModel

    def delete_file(self, filename: str, data: dict):
        meta_count, meta_deleted = ExplorersMetaJson.objects.filter(
            dashboard_name=data.get("explorer_name")
        ).delete()
        explorer_count, explorer_deleted = ExplorersUpdate.objects.filter(
            explorer=data.get("explorer_name")
        ).delete()
        meta_deleted.update(explorer_deleted)
        return meta_count + explorer_count, meta_deleted

    def update_or_create_meta(self, filename: str, metadata: ExplorerValidateModel):
        updated_values = {
            "dashboard_meta": metadata.model_dump(),
            "route": metadata.route,
            "sites": metadata.sites,
        }
        obj, created = ExplorersMetaJson.objects.update_or_create(
            dashboard_name=metadata.explorer_name,
            defaults=updated_values,
        )

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
                    source = table_list[k].get("source")
                    upd, create = ExplorersUpdate.objects.update_or_create(
                        explorer=exp_name,
                        file_name=k,
                        defaults={"last_update": last_update},
                    )

                    if table_operation == "SLEEP":
                        continue
                    elif table_operation == "REBUILD":
                        obj.populate_db(table=table_name, source=source, rebuild=True)
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

        # need to revalidate the MetaJsons separately, since routes/sites info are stored in MetaJson, not ExplorersUpdate
        self.revalidate_route(created_objects)

        return successful_meta


class PublicationBuilder(GeneralMetaBuilder):
    CATEGORY = "PUBLICATION"
    MODEL = Publication
    GITHUB_DIR = "pub-dosm/publications"
    VALIDATOR = PublicationValidateModel

    def delete_file(self, filename: str, data: dict):
        """
        This will also delete the download counts for relevant publication resource!
        """
        return Publication.objects.filter(
            publication_id=data.get("publication")
        ).delete()

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
                "description_email": metadata.en.description_email,
                "release_date": metadata.release_date,
                "frequency": metadata.frequency,
                "geography": metadata.geography,
                "demography": metadata.demography,
            },
        )

        # delete outdated publication resources (retain based on avail `resource_id`)
        valid_pub_resource_ids = [
            resource.resource_id for resource in metadata.en.resources
        ]
        del_total, del_objects = (
            PublicationResource.objects.filter(publication=pub_object_en)
            .exclude(resource_id__in=valid_pub_resource_ids)
            .delete()
        )

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
            ],
            update_conflicts=True,
            unique_fields=["resource_id", "publication"],
            update_fields=[
                "resource_type",
                "resource_name",
                "resource_link",
            ],
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
                "description_email": metadata.bm.description_email,
                "release_date": metadata.release_date,
                "frequency": metadata.frequency,
                "geography": metadata.geography,
                "demography": metadata.demography,
            },
        )

        # delete outdated publication resources (retain based on avail `resource_id`)
        valid_pub_resource_ids = [
            resource.resource_id for resource in metadata.bm.resources
        ]
        del_total, del_objects = (
            PublicationResource.objects.filter(publication=pub_object_bm)
            .exclude(resource_id__in=valid_pub_resource_ids)
            .delete()
        )

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
            ],
            update_conflicts=True,
            unique_fields=["resource_id", "publication"],
            update_fields=[
                "resource_type",
                "resource_name",
                "resource_link",
            ],
        )

        # Send notification to all subscribers of the publication type only if release date is today
        if not metadata.abort_email and metadata.release_date == date.today():
            try:
                subscriptions = Subscription.objects.filter(
                    publications__overlap=[metadata.publication_type, 'all']
                )
                triggers.send_telegram(
                    f'List of subscribers to get email notif on {metadata.en.title}:'
                    f'{[s.email for s in subscriptions]}'
                )
                for subscriber in subscriptions:
                    publication_id = metadata.publication
                    SubscriptionEmail(subscriber, publication_id).send_email()

            except Subscription.DoesNotExist:
                triggers.send_telegram(
                    f'No one subscribed to {metadata.publication_type}. No email will be send.'
                )
        return [pub_object_en, pub_object_bm]


class PublicationDocumentationBuilder(GeneralMetaBuilder):
    CATEGORY = "PUBLICATION_DOCS"
    MODEL = PublicationDocumentation
    GITHUB_DIR = "pub-dosm/documentation"
    VALIDATOR = PublicationDocumentationValidateModel

    def delete_file(self, filename: str, data: dict):
        """
        This will also delete the download counts for relevant publication documentation resource!
        """
        return PublicationDocumentation.objects.filter(
            publication_id=data.get("publication")
        ).delete()

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

        # delete outdated publication resources (retain based on avail `resource_id`)
        valid_ids = [resource.resource_id for resource in metadata.en.resources]
        del_total, del_objects = (
            PublicationDocumentationResource.objects.filter(publication=pub_object_en)
            .exclude(resource_id__in=valid_ids)
            .delete()
        )

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
            ],
            update_conflicts=True,
            unique_fields=["resource_id", "publication"],
            update_fields=[
                "resource_type",
                "resource_name",
                "resource_link",
            ],
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

        valid_ids = [resource.resource_id for resource in metadata.bm.resources]
        del_total, del_objects = (
            PublicationDocumentationResource.objects.filter(publication=pub_object_bm)
            .exclude(resource_id__in=valid_ids)
            .delete()
        )

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
            ],
            update_conflicts=True,
            unique_fields=["resource_id", "publication"],
            update_fields=[
                "resource_type",
                "resource_name",
                "resource_link",
            ],
        )

        return [pub_object_en, pub_object_bm]


class PublicationUpcomingBuilder(GeneralMetaBuilder):
    CATEGORY = "PUBLICATION_UPCOMING"
    MODEL = PublicationUpcoming
    GITHUB_DIR = "pub-dosm/upcoming"
    VALIDATOR = PublicationUpcomingValidateModel

    def delete_file(self, filename: str, data: dict):
        """
        Deletes the whole table because only a single file is supposed to reside within pub-dosm/upcoming
        """
        return PublicationUpcoming.objects.all().delete()

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


class PublicationTypeBuilder(GeneralMetaBuilder):
    CATEGORY = "PUBLICATION_TYPE"
    MODEL = PublicationType
    GITHUB_DIR = "pub-dosm/pubs"
    VALIDATOR = PublicationTypeValidateModel

    def delete_file(self, filename: str, data: dict):
        """
        Deletes the whole table because only a single file is supposed to reside within pub-dosm/pubs
        """
        pub_type = PublicationType.objects.all().delete()
        return pub_type

    def update_or_create_meta(
            self, filename: str, metadata: PublicationTypeValidateModel
    ):
        df_type = pd.read_parquet(metadata.parquet_link)
        df_subtype = pd.read_parquet(metadata.parquet_link)

        PublicationType.objects.all().delete()
        PublicationSubtype.objects.all().delete()

        # populate PublicationType
        pub_type_list = []
        for index, row in df_type.iterrows():
            try:
                PublicationType.objects.get(order=row['type_order'], id=row['type'])
            except PublicationType.DoesNotExist:
                pub_type = PublicationType.objects.create(
                    order=row['type_order'],
                    id=row['type'],
                    type_en=row['type_en'],
                    type_bm=row['type_bm'])
                pub_type_list.append(pub_type)

        # populate PublicationSubtype
        pub_subtype_list = []
        for index, row in df_subtype.iterrows():
            pub_type = PublicationType.objects.get(order=row['type_order'], id=row['type'])
            try:
                PublicationSubtype.objects.get(
                    publication_type=pub_type,
                    order=row['subtype_order'],
                    id=row['subtype']
                )
            except PublicationSubtype.DoesNotExist:
                pub_subtype = PublicationSubtype.objects.create(
                    publication_type=pub_type,
                    id=row['subtype'],
                    order=row['subtype_order'],
                    subtype_en=row['subtype_en'],
                    subtype_bm=row['subtype_bm'])
                pub_subtype_list.append(pub_subtype)

        # arrange all in PublicationType
        for type in pub_type_list:
            dict_en = {}
            dict_bm = {}
            for subtype in type.publicationsubtype_set.all().order_by("order"):
                dict_en[subtype.id] = subtype.subtype_en
                dict_bm[subtype.id] = subtype.subtype_bm
            type.dict_en = dict_en
            type.dict_bm = dict_bm
            type.save()

        return pub_type_list + pub_subtype_list
