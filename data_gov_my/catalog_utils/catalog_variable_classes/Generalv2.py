class GeneralChartsUtil:
    # Data from file
    full_meta = {}
    file_data = {}
    cur_data = {}
    cur_catalog_data = {}
    all_variable_data = []
    file_src = ""

    # Catalog Data info
    catalog_filters = {}
    metadata_neutral = {}
    metadata_lang = {}
    chart = {}

    # Additional API data
    precision = 1
    translations = {"en": {}, "bm": {}}
    data_frequency = ""

    # Identifiers
    unique_id = ""
    cur_id = 0
    result = {}
    db_input = {}
    variable_name = ""

    # file stuff
    read_from = ""

    # End results
    chart_details = {}
    explanation = {}
    metadata = {}
    downloads = {}
    api = {}

    """
    Initiailize the general information for a catalog variable
    """

    def __init__(self, full_meta, file_data, cur_data, all_variable_data, file_src):
        # self.general_meta_validation()

        # Sets all current attributes
        self.full_meta = full_meta
        self.file_data = file_data
        self.cur_data = cur_data
        self.cur_catalog_data = cur_data["catalog_data"]
        self.catalog_filters = self.cur_catalog_data["catalog_filters"]
        self.data_frequency = self.catalog_filters["frequency"]
        self.metadata_neutral = self.cur_catalog_data["metadata_neutral"]
        self.metadata_lang = self.cur_catalog_data["metadata_lang"]
        self.chart = self.cur_catalog_data["chart"]
        self.all_variable_data = all_variable_data
        self.file_src = file_src

        # Decides which link to read from
        if "link_preview" in file_data:
            self.read_from = file_data["link_preview"]
        elif "link_parquet" in file_data:
            self.read_from = file_data["link_parquet"]

        # Sets current (int) id of object
        self.cur_id = self.cur_data["id"]

        # Creates the unique id
        bucket_name = self.file_data["bucket"]
        file_name = self.file_data["file_name"].replace(".parquet", "")
        cur_id_str = str(self.cur_id)
        self.unique_id = f"{bucket_name}_{file_name}_{cur_id_str}"

        # Sets the current variable name
        self.variable_name = self.cur_data["name"]

        # Creates the database dictionary input
        self.db_input = self.build_db_info()

        # Creates segments for API
        self.explanation = self.metadata_lang
        self.metadata = self.build_metadata_info()
        self.downloads = self.build_downloads_info()
        self.chart_details = self.build_intro_info()

        # Sets precision attribute for API
        self.set_precision()
        self.get_translations()
        self.get_queries()

    """
    Gets a nested dictionary key
    """

    def get_dict(self, d, keys):
        for key in keys:
            d = d[key]
        return d

    """
    Sets a nested dictionary key
    """

    def set_dict(self, d, keys, value):
        d = self.get_dict(d, keys[:-1])
        d[keys[-1]] = value

    """
    Get precision info
    """

    def set_precision(self):
        if "precision" in self.chart["chart_filters"]:
            self.precision = self.chart["chart_filters"]["precision"]

    """
    Get language format
    """

    def get_translations(self):
        if "translations" in self.cur_catalog_data:
            self.translations = self.cur_catalog_data["translations"]

    def get_queries(self):
        if "queries" in self.cur_catalog_data:
            self.queries = self.cur_catalog_data["queries"]

    """
    Builds the variables table for each catalog variable
    """

    def build_variable_table(self, dict_data):
        res = []

        key_list = list(dict_data.keys())

        for i in range(0, len(dict_data["x"])):
            data = {}
            for k in key_list:
                data[k] = dict_data[k][i]
            res.append(data)

        return res

    """
    Formats and builds, the metadata for each variable
    """

    def build_metadata_info(self):
        res = {}
        res["metadata"] = self.metadata_neutral
        res["metadata"]["dataset_desc"] = self.file_data["description"]
        res["metadata"]["data_source"] = self.catalog_filters["data_source"]
        res["metadata"]["in_dataset"] = []
        res["metadata"]["out_dataset"] = []
        res["metadata"]["url"] = {}

        for i in ["csv", "parquet"]:
            link = ""
            if f"link_{i}" in self.file_data:
                link = self.file_data[f"link_{i}"]
            res["metadata"]["url"][i] = link

        bucket_name = self.file_data["bucket"]
        file_name = self.file_data["file_name"].replace(".parquet", "")

        for v in self.all_variable_data:
            v_id = str(v["id"])
            if v["id"] != -1:
                v["unique_id"] = f"{bucket_name}_{file_name}_{v_id}"

            append_to = "out_dataset"

            if v["id"] == self.cur_id:
                v.pop("catalog_data")
                append_to = "in_dataset"

            if v["id"] != 0:
                res["metadata"][append_to].append(v)

        return res["metadata"]

    """
    Formats and builds, the downloads for each variable
    """

    def build_downloads_info(self):
        res = {}
        res["downloads"] = {}
        res["downloads"]["csv"] = self.metadata["url"]["csv"]
        res["downloads"]["parquet"] = self.metadata["url"]["parquet"]

        return res["downloads"]

    """
    Helper to build the filters within an API
    """

    def build_api_object_filter(self, key, def_val, options):
        # If it is a dictionary, convert to list of objects
        # if isinstance(options, dict):
        #     options = [{"label": k, "value": v} for k, v in options.items()]

        filter = {}
        filter["key"] = key
        filter["default"] = def_val
        filter["options"] = options

        return filter

    """
    Formats and builds, the intro for each variable
    """

    def build_intro_info(self):
        intro = {}
        intro["intro"] = {}
        intro["intro"]["id"] = self.cur_id
        intro["intro"]["unique_id"] = self.unique_id
        intro["intro"]["name"] = self.variable_name

        for lang in ["en", "bm"]:
            intro["intro"][lang] = {}
            intro["intro"][lang]["title"] = self.cur_data[f"title_{lang}"]
            intro["intro"][lang]["desc"] = self.cur_data[f"desc_{lang}"]

        return intro

    """
    Formats and builds, the database insertion for each variable
    """

    def build_db_info(self):
        res = {}
        res["catalog_meta"] = self.full_meta

        # Sets catalog names
        catalog_name_en = self.cur_data["title_en"]
        catalog_name_bm = self.cur_data["title_bm"]
        res["catalog_name"] = f"{catalog_name_en} | {catalog_name_bm}"

        # Sets catalog category
        res["catalog_category"] = self.file_data["category"]
        catalog_category_en = self.file_data["category_en"]
        catalog_category_bm = self.file_data["category_bm"]
        res["catalog_category_name"] = f"{catalog_category_en} | {catalog_category_bm}"

        # Sets catalog subcategory
        res["catalog_subcategory"] = self.file_data["subcategory"]
        catalog_subcategory_en = self.file_data["subcategory_en"]
        catalog_subcategory_bm = self.file_data["subcategory_bm"]
        res[
            "catalog_subcategory_name"
        ] = f"{catalog_subcategory_en} | {catalog_subcategory_bm}"

        # Sets the frequency of the catalog
        res["time_range"] = self.catalog_filters["frequency"]

        # Sets the geographic, demographic and datasource ( multiple )
        res["geographic"] = " | ".join(self.catalog_filters["geographic"])
        res["demographic"] = " | ".join(self.catalog_filters["demographic"])
        res["data_source"] = " | ".join(self.catalog_filters["data_source"])

        # Sets the range of start and end of dataset
        res["dataset_begin"] = int(self.catalog_filters["start"])
        res["dataset_end"] = int(self.catalog_filters["end"])

        # Sets the file source
        res["file_src"] = self.file_src

        return res

    """
    Formats and builds, the overall catalog information for the API
    """

    def build_catalog_data_info(self):
        res = {}
        res["API"] = self.api
        res["explanation"] = self.explanation
        res["metadata"] = self.metadata
        res["downloads"] = self.downloads
        res["chart_details"] = self.chart_details
        res["translations"] = self.translations
        res["queries"] = self.queries

        return res

    """
    General purpose method to cross-check between data-types required
    """

    def validate_data_type(self, fields, src, src_level):
        for f, v in fields.items():
            for i in v:
                if type(src_level[i]).__name__ != f:
                    raise Exception(
                        "Source : " + src + " Key : '" + i + "', should be a " + f + "!"
                    )

    """
    General purpose method to check for the presence of a field
    """

    def validate_field_presence(self, fields, src, src_level):
        for f in fields:
            if f not in src_level:
                raise Exception(
                    "Source : " + src + " Key : '" + f + "', not found in meta!"
                )

    """
    General validation to validate all general-fields within a data catalog
    """

    def general_meta_validation(self):
        data = self.full_meta
        src = self.file_src

        self.validate_field_presence(["file"], src, data)
        s = {"dict": ["file"]}
        self.validate_data_type(s, src, data)

        self.validate_field_presence(["variables"], src, data["file"])
        s = {"list": ["variables"]}
        self.validate_data_type(s, src, data["file"])

        self.validate_field_presence(
            [
                "category",
                "category_en",
                "category_bm",
                "file_name",
                "bucket",
                "description",
            ],
            src,
            data["file"],
        )
        s = {
            "str": [
                "category",
                "category_en",
                "category_bm",
                "file_name",
                "bucket",
            ],
            "dict": ["description"],
        }
        self.validate_data_type(s, src, data["file"])

        self.validate_field_presence(["en", "bm"], src, data["file"]["description"])
        s = {"str": ["en", "bm"]}
        self.validate_data_type(s, src, data["file"]["description"])
