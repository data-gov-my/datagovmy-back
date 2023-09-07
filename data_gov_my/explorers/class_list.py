from data_gov_my.explorers import NamePopularity, BirthdayPopularity, Elections
from data_gov_my.explorers.General import General_Explorer
from data_gov_my.explorers.KTMB import KTMB
from data_gov_my.explorers.Prasarana import Prasarana

EXPLORERS_CLASS_LIST: dict[str, General_Explorer] = {
    "NAME_POPULARITY": NamePopularity.NAME_POPULARITY,
    "BIRTHDAY_POPULARITY": BirthdayPopularity.BIRTHDAY_POPULARITY,
    "ELECTIONS": Elections.ELECTIONS,
    "KTMB": KTMB,
    "Prasarana": Prasarana,
}
