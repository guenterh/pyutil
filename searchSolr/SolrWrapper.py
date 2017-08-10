
from config.appConfig import AppConfig
from string import Template
import requests, json


class SolrWrapper:


    def __init__(self, config: AppConfig):
        self.sorlURL = config.getConfig()["SolrURL"]
        self.template = Template(self.sorlURL)


    def checkId(self, currentId):

        url = self.template.substitute({"id":currentId})
        response = requests.get(url)
        data = json.loads(response.text)
        number = data["response"]["numFound"]

        return number > 0
