import importlib
import json
import logging
import os
from abc import ABC, abstractmethod
from os.path import isfile
from typing import List

from django.core.cache import cache
from django.core.exceptions import FieldDoesNotExist

from data_gov_my.models import (
    CatalogJson,
    DashboardJson,
    FormTemplate,
    MetaJson,
    i18nJson,
)
from data_gov_my.utils import common, dashboard_builder, triggers
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
)


class GeneralMetaBuilder(ABC):
    CATEGORY = ["DATA_CATALOG", "DASHBOARDS", "I18N", "FORMS"]
    GITHUB_DIR = ["catalog", "dashboards", "i18n", "forms"]
    MODEL = ""

    @staticmethod
    def build_operation_by_category(
        manual=True, category=None, rebuild=True, meta_files=[]
    ):
        """
        Function used to find the concrete children class builder, e.g. DashboardBuilder, and peform building operations accordingly.
        """
        builder_classes = {
            "DASHBOARDS": DashboardBuilder,
            "I18N": i18nBuilder,
            "FORMS": FormBuilder,
            "DATA_CATALOG": DataCatalogBuilder,
        }
        Builder: GeneralMetaBuilder = builder_classes.get(category, None)
        if Builder:
            Builder().build_operation(
                manual=manual, rebuild=rebuild, meta_files=meta_files
            )

    @staticmethod
    def selective_update():
        """
        Selectively update database objects based on the latest commit in the github repository.
        """
        latest_sha = get_latest_info_git("SHA", "")
        data = json.loads(get_latest_info_git("COMMIT", latest_sha))
        changed_files = [f["filename"] for f in data["files"]]
        filtered_changes = GeneralMetaBuilder.filter_changed_files(changed_files)
        category_mapper = dict(
            zip(GeneralMetaBuilder.GITHUB_DIR, GeneralMetaBuilder.CATEGORY)
        )

        for category, files in filtered_changes.items():
            if files:
                GeneralMetaBuilder.build_operation_by_category(
                    manual=False,
                    category=category_mapper[category],
                    rebuild=False,
                    meta_files=files,
                )

    @staticmethod
    def filter_changed_files(file_list) -> dict:
        """
        Maps the files to respective categories and returns a dictionary where the keys are the categories and the values are the corresponding files.
        """
        changed_files = {category: [] for category in GeneralMetaBuilder.GITHUB_DIR}
        meta_dir = os.path.join("DATAGOVMY_SRC", os.getenv("GITHUB_DIR", "-"))
        # clone meta repo if needed
        if not os.path.exists(meta_dir):
            GeneralMetaBuilder.refresh_meta_repo()

        for f in file_list:
            f_path = os.path.join(meta_dir, f)
            f_info = f.split("/")
            if (
                len(f_info) > 1
                and f_info[0] in changed_files
                and os.path.exists(f_path)
            ):
                changed_files[f_info[0]].append(
                    os.path.join(*f_info[1:]).replace(".json", "")
                )

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

        routes = []

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
            successful_routes = []
            failed_routes = []
            failed_info = []
            telegram_msg = (
                triggers.format_header(
                    f"<code>{str(model_obj).upper()}</code> REVALIDATION STATUS"
                )
                + "\n"
            )
            routes = model_obj.route
            if not routes:  # current object does not have any routes
                continue
            response = revalidate_frontend(routes=routes)
            if response and response.status_code == 200:
                successful_routes.extend(response.json()["revalidated"])
            elif response and response.status_code == 400:
                failed_routes.extend(routes.split(","))
                failed_info.append(response.json())
            else:
                failed_routes.extend(routes.split(","))
                failed_info.append({"DB OBJECT": str(model_obj), "ERROR": "Unknown :("})

            telegram_msg += triggers.format_files_with_status_emoji(
                successful_routes, "✅︎"
            )
            telegram_msg += "\n\n"
            telegram_msg += triggers.format_files_with_status_emoji(failed_routes, "❌")

            if len(failed_info) > 0:
                telegram_msg += "\n\n"
                telegram_msg += triggers.format_multi_line(
                    failed_info, "FAILED REVALIDATION INFO"
                )

            triggers.send_telegram(telegram_msg)

    def build_operation(self, manual=True, rebuild=True, meta_files=[]):
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
        refreshed = self.refresh_meta_repo()
        if not refreshed:
            logging.warning("Github repo has not been refreshed, abort building")
            return False

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
                created_object = self.update_or_create_meta(meta, data)
                if isinstance(created_object, list):
                    for object in created_object:
                        object.save()
                    meta_objects.extend(created_object)
                else:
                    created_object.save()
                    meta_objects.append(created_object)
                f.close()
            except Exception as e:
                failed.append({"FILE": meta, "ERROR": e})

        telegram_msg = [
            triggers.format_header(f"Meta Built Status ({self.MODEL.__name__})"),
            triggers.format_files_with_status_emoji(meta_objects, "✅︎") + "\n",
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
        except FieldDoesNotExist as e:
            return False


class DashboardBuilder(GeneralMetaBuilder):
    CATEGORY = "DASHBOARDS"
    MODEL = MetaJson
    GITHUB_DIR = "dashboards"

    def update_or_create_meta(self, filename: str, metadata: dict):
        route = metadata.get("route", "")
        updated_values = {"dashboard_meta": metadata, "route": route}
        obj, created = MetaJson.objects.update_or_create(
            dashboard_name=metadata["dashboard_name"],
            defaults=updated_values,
        )
        cache.set("META_" + metadata["dashboard_name"], metadata)
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
                    # failed_notify.append(meta)
                    failed_obj = {}
                    failed_obj["DASHBOARD"] = dbd_name
                    failed_obj["CHART_NAME"] = chart_name
                    failed_obj["ERROR"] = str(e)
                    # failed_builds.append(failed_obj)
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

    def update_or_create_meta(self, filename: str, metadata: dict):
        language, filename = os.path.split(filename)
        filename = filename.replace(".json", "")

        obj, created = i18nJson.objects.update_or_create(
            filename=filename, language=language, defaults=metadata
        )
        return obj


class FormBuilder(GeneralMetaBuilder):
    CATEGORY = "FORMS"
    MODEL = FormTemplate
    GITHUB_DIR = "forms"

    def update_or_create_meta(self, filename: str, metadata: dict):
        form_type = filename.replace(".json", "")
        return FormTemplate.create(form_type=form_type, form_meta=metadata)


class DataCatalogBuilder(GeneralMetaBuilder):
    CATEGORY = "DATA_CATALOG"
    MODEL = CatalogJson
    GITHUB_DIR = "catalog"

    def update_or_create_meta(self, filename: str, metadata: dict):
        file_data = metadata["file"]
        all_variable_data = metadata["file"]["variables"]
        full_meta = metadata
        file_src = filename.replace(".json", "")

        created_objects = []

        for cur_data in all_variable_data:
            if "catalog_data" in cur_data:  # Checks if the catalog_data is in
                cur_catalog_data = cur_data["catalog_data"]
                chart_type = cur_catalog_data["chart"]["chart_type"]

                if chart_type in common.CHART_TYPES:
                    args = {
                        "full_meta": full_meta,
                        "file_data": file_data,
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

                    db_obj, created = CatalogJson.objects.update_or_create(
                        id=unique_id, defaults=db_input
                    )
                    created_objects.append(db_obj)

        return created_objects
