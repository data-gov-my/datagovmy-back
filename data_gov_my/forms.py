from django.forms import ModelForm
from data_gov_my.models import ModsData


class ModsDataForm(ModelForm):

    class Meta:
        model = ModsData
        fields = "__all__"
