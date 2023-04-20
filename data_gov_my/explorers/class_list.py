from data_gov_my.explorers import NamePopularity, BirthdayPopularity, Elections
import typing
from data_gov_my.explorers.General import General_Explorer

EXPLORERS_CLASS_LIST: typing.Dict[str, General_Explorer]= {
    'NAME_POPULARITY' : NamePopularity.NAME_POPULARITY,
    'BIRTHDAY_POPULARITY': BirthdayPopularity.BIRTHDAY_POPULARITY,
    'ELECTIONS' : Elections.ELECTIONS
}