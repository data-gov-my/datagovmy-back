from django.core.cache import cache
from django.core.cache import caches
from data_gov_my.models import ViewCount


class ViewCountCache:

    cache_choice = caches["viewcount"]

    # Cache key naming convention
    cache_key_prepend = "viewcount_"

    # Default cache structure
    default_view_value = {
        "type": "",
        "view_count": 0,
        "download_csv": 0,
        "download_parquet": 0,
        "download_png": 0,
        "download_svg": 0,
    }

    """
    Constructor
    """

    def __init__(self):
        pass

    """
    Handle cache request
    """

    def handle_viewcount(self, id, type, metric):
        cache_key = f"{self.cache_key_prepend}{id}"
        cur_val = self.cache_choice.get(cache_key)

        # If the cur_val isn't present in cache
        if not cur_val:
            cur_val = ViewCount.objects.filter(id=id).values()
            if cur_val.exists():
                cur_val = cur_val[0]
                cur_val[metric] += 1
                self.cache_choice.set(cache_key, cur_val)
            # If the key has never existed
            else:
                cur_val = dict(self.default_view_value)
                cur_val["id"] = id
                cur_val["type"] = type
                cur_val[metric] = 1
                self.cache_choice.set(cache_key, cur_val)
        else:
            with self.cache_choice.lock(cache_key):
                cur_val[metric] += 1
                self.cache_choice.set(cache_key, cur_val)

        return cur_val

    """
    Update cache in db
    """

    @classmethod
    def update_cache(self):
        try:
            all_keys = self.cache_choice.keys("*")
            objs = []

            for c in all_keys:
                if self.cache_key_prepend in c:
                    with self.cache_choice.lock(c):
                        val = self.cache_choice.get(c)
                        objs.append(ViewCount(**val))

            if objs:
                ViewCount.objects.bulk_create(
                    objs=objs,
                    update_conflicts=True,
                    unique_fields=["id"],
                    update_fields=[
                        "view_count",
                        "download_csv",
                        "download_parquet",
                        "download_png",
                        "download_svg",
                    ],
                )

                all_view_counts = list(ViewCount.objects.all().values())

                # Sets all cache to updated values from db
                for x in all_view_counts:
                    self.cache_choice.set(f"{self.cache_key_prepend}{x['id']}", x)

            return True

        except Exception as e:
            return False
