import json
from os.path import isfile, join
import os
from data_gov_my.utils.cron_utils import *
import logging
from abc import ABC, abstractmethod
from django.db import models
from django.core.cache import cache
from django.core.exceptions import FieldDoesNotExist
from data_gov_my.models import DashboardJson, FormTemplate, MetaJson, i18nJson
from data_gov_my.utils import dashboard_builder, triggers
from typing import List
from data_gov_my.utils.common import LANGUAGE_CHOICES


class GeneralDataBuilder(ABC):
    CATEGORIES = ["DATA_CATALOG", "DASHBOARDS", "I18N", "FORMS"]
    GITHUB_DIR = ""
    MODEL = ""
    REVALIDATE = False

    @staticmethod
    def refresh_meta_repo():
        """
        Delete local github folder (if any), and re-populate
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

    def format_failed_object(self, data_name, error):
        return f"{data_name}: {error}"

    def get_operation_files(operation):
        opr = operation.split(" ")
        chosen_opr = opr[0]
        files = []

        if len(opr) > 1:
            files = opr[1].split(",")

        return {"operation": chosen_opr, "files": files}

    def get_github_directory(self):
        return os.path.join(
            os.getcwd(),
            "DATAGOVMY_SRC",
            os.getenv("GITHUB_DIR", "-"),
            self.GITHUB_DIR,
        )

    def get_meta_files(self):
        meta_dir = self.get_github_directory()
        return [f for f in os.listdir(meta_dir) if isfile(os.path.join(meta_dir, f))]

    def additional_handling(self, rebuild: bool, meta_files, created_objects):
        pass

    @abstractmethod
    def update_or_create_meta(self, filename: str, metadata: dict):
        """
        Returns a list of failed objects
        """
        pass

    def revalidate_route(self, objects):
        """
        Only applicable if self.model have "route" field
        """
        routes = []
        for model_obj in objects:
            routes.append(model_obj.route)

        revalidate_status = revalidate_frontend(routes=routes)
        print("REVALIDATE STATUS:", routes, revalidate_status)
        # TODO: format telegram for revaliadtion status

    def build_operation(self, rebuild=True, meta_files=[]):
        refreshed = self.refresh_meta_repo()
        if not refreshed:
            logging.warning("Github repo has not been refreshed, abort building")
            return False

        # get the operation files
        if rebuild:
            self.MODEL.objects.all().delete()

        # get meta files (prioritise input files)
        meta_files = (
            [f + ".json" for f in meta_files] if meta_files else self.get_meta_files()
        )
        failed = []
        meta_objects = []
        for meta in meta_files:
            try:
                f_meta = os.path.join(self.get_github_directory(), meta)
                f = open(f_meta)
                data = json.load(f)
                created_object = self.update_or_create_meta(meta, data)
                created_object.save()
                meta_objects.append(created_object)
            except Exception as e:
                failed.append({"file": meta, "error": e})

        # iterate through each meta "blueprint" and populate DB accordingly
        self.additional_handling(rebuild, meta_files, meta_objects)

        # TODO: catch exception - telegram inform
        # TODO: revalidate routes

        if self.model_has_field("route"):
            self.revalidate_route(meta_objects)

        # TODO: telegram inform successes

    def model_has_field(self, field):
        try:
            field = self.MODEL._meta.get_field(field)
            return True
        except FieldDoesNotExist as e:
            return False


class DashboardBuilder(GeneralDataBuilder):
    CATEGORY = "DASHBOARD"
    MODEL = MetaJson
    GITHUB_DIR = "dashboards"

    def update_or_create_meta(self, filename: str, metadata: dict):
        """
        meta represents the dashboard metajson file, e.g. blood_donation.json
        """
        updated_values = {"dashboard_meta": metadata}
        route = metadata.get("route", "")
        obj, created = MetaJson.objects.update_or_create(
            dashboard_name=metadata["dashboard_name"],
            route=route,
            defaults=updated_values,
        )
        cache.set("META_" + metadata["dashboard_name"], metadata)
        return obj

    def additional_handling(
        self, rebuild: bool, meta_files, created_objects: List[MetaJson]
    ):
        """
        Dashboard additionally will populate DashboardJson objects based on each charts defined in metajson.
        Additionally revalidates the routes
        """
        if rebuild:
            DashboardJson.objects.all().delete()

        dashboard_list = set()
        revalidate_routes = []

        for meta in created_objects:
            dbd_meta: dict = meta.dashboard_meta
            dbd_name = meta.dashboard_name
            revalidate_routes.append(meta.route)
            chart_list = dbd_meta["charts"]

            dashboard_list.add(dbd_name)

            for k in chart_list.keys():
                chart_name = k
                chart_type = chart_list[k]["chart_type"]
                c_data = {}
                c_data["variables"] = chart_list[k]["variables"]
                c_data["input"] = chart_list[k]["chart_source"]
                api_type = chart_list[k]["api_type"]
                try:
                    res = {}
                    res["data"] = dashboard_builder.build_chart(chart_type, c_data)
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
                        cache.set(dbd_name + "_" + k, res)
                except Exception as e:
                    # failed_notify.append(meta)
                    failed_obj = {}
                    failed_obj["CHART_NAME"] = chart_name
                    failed_obj["DASHBOARD"] = dbd_name
                    failed_obj["ERROR"] = str(e)
                    # failed_builds.append(failed_obj)
                    print(failed_obj)

        # TODO: telegram notif: success or failure (list of failed charts)


class i18nBuilder(GeneralDataBuilder):
    CATEGORY = "I18N"
    MODEL = i18nJson
    GITHUB_DIR = "i18n"

    def get_meta_files(self):
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
        """
        meta represents the i18n metajson file, e.g. en-GB/blood_donation.json
        """
        language, filename = os.path.split(filename)
        filename = filename.replace(".json", "")

        obj, created = i18nJson.objects.update_or_create(
            filename=filename, language=language, defaults=metadata
        )
        return obj


class FormBuilder(GeneralDataBuilder):
    CATEGORY = "Form"
    MODEL = FormTemplate
    GITHUB_DIR = "forms"

    def update_or_create_meta(self, filename: str, metadata: dict):
        """
        meta represents the dashboard metajson file, e.g. mods.json
        """
        form_type = filename.replace(".json", "")
        return FormTemplate.create(form_type=form_type, form_meta=metadata)
