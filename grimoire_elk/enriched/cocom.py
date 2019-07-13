# -*- coding: utf-8 -*-
#
# Copyright (C) 2015-2019 Bitergia
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
#
# Authors:
#   Valerio Cosentino <valcos@bitergia.com>
#   Nishchith Shetty <inishchith@gmail.com>
#

import logging
from .enrich import Enrich, metadata
from grimoirelab_toolkit.datetime import str_to_datetime
from grimoire_elk.elastic import ElasticSearch

MAX_SIZE_BULK_ENRICHED_ITEMS = 200

logger = logging.getLogger(__name__)


class CocomEnrich(Enrich):

    def __init__(self, db_sortinghat=None, db_projects_map=None, json_projects_map=None,
                 db_user='', db_password='', db_host=''):
        super().__init__(db_sortinghat, db_projects_map, json_projects_map,
                         db_user, db_password, db_host)

        self.studies = []
        self.studies.append(self.enrich_repo_analysis)

    def get_identities(self, item):
        """ Return the identities from an item """
        identities = []

        return identities

    def has_identities(self):
        """ Return whether the enriched items contains identities """

        return False

    def get_field_unique_id(self):
        return "id"

    def extract_modules(self, file_path):
        """ Extracts module path from the given file path """
        path_chunks = file_path.split('/')

        modules = []
        for idx in range(len(path_chunks)):
            sub_path = '/'.join(path_chunks[:idx])

            if sub_path:
                modules.append(sub_path)

        return modules

    @metadata
    def get_rich_item(self, file_analysis):

        eitem = {}

        eitem['ccn'] = file_analysis.get("ccn", None)
        eitem['num_funs'] = file_analysis.get("num_funs", None)
        eitem['tokens'] = file_analysis.get("tokens", None)
        eitem['loc'] = file_analysis.get("loc", None)
        eitem['ext'] = file_analysis.get("ext", None)
        eitem['in_commit'] = file_analysis.get("in_commit", None)
        eitem['blanks'] = file_analysis.get("blanks", None)
        eitem['comments'] = file_analysis.get("comments", None)
        eitem['file_path'] = file_analysis.get("file_path", None)
        eitem['modules'] = self.extract_modules(eitem['file_path'])
        eitem = self.__add_derived_metrics(file_analysis, eitem)

        return eitem

    def get_rich_items(self, item):
        # The real data
        entry = item['data']

        enriched_items = []

        for file_analysis in entry["analysis"]:
            eitem = self.get_rich_item(file_analysis)

            for f in self.RAW_FIELDS_COPY:
                if f in item:
                    eitem[f] = item[f]
                else:
                    eitem[f] = None

            # common attributes
            eitem['commit_sha'] = entry['commit']
            eitem['author'] = entry['Author']
            eitem['committer'] = entry['Commit']
            eitem['message'] = entry['message']
            eitem['author_date'] = self.__fix_field_date(entry['AuthorDate'])
            eitem['commit_date'] = self.__fix_field_date(entry['CommitDate'])

            if self.prjs_map:
                eitem.update(self.get_item_project(eitem))

            # uuid
            eitem['id'] = "{}_{}".format(eitem['commit_sha'], eitem['file_path'])

            eitem.update(self.get_grimoire_fields(entry["AuthorDate"], "file"))

            self.add_repository_labels(eitem)
            self.add_metadata_filter_raw(eitem)

            enriched_items.append(eitem)

        return enriched_items

    def __add_derived_metrics(self, file_analysis, eitem):
        """ Add derived metrics fields """
        if eitem['loc']:
            total_lines = eitem['loc'] + eitem['comments'] + eitem['blanks']
            eitem["comments_ratio"] = eitem['comments'] / total_lines
            eitem["blanks_ratio"] = eitem['blanks'] / total_lines
        else:
            eitem["comments_ratio"] = eitem['comments']
            eitem["blanks_ratio"] = eitem['blanks']

        return eitem

    def enrich_items(self, ocean_backend, events=False):
        items_to_enrich = []
        num_items = 0
        ins_items = 0

        for item in ocean_backend.fetch():
            rich_items = self.get_rich_items(item)

            items_to_enrich.extend(rich_items)
            if len(items_to_enrich) < MAX_SIZE_BULK_ENRICHED_ITEMS:
                continue

            num_items += len(items_to_enrich)
            ins_items += self.elastic.bulk_upload(items_to_enrich, self.get_field_unique_id())
            items_to_enrich = []

        if len(items_to_enrich) > 0:
            num_items += len(items_to_enrich)
            ins_items += self.elastic.bulk_upload(items_to_enrich, self.get_field_unique_id())

        if num_items != ins_items:
            missing = num_items - ins_items
            logger.error("%s/%s missing items for Cocom", str(missing), str(num_items))
        else:
            logger.info("%s items inserted for Cocom", str(num_items))

        return num_items

    def enrich_repo_analysis(self, ocean_backend, enrich_backend, no_incremental=False,
                             out_index="cocom_enrich_graal_repo",
                             date_field="grimoire_creation_date"):

        logger.info("Doing enrich_repository_analysis study for index {}"
                    .format(self.elastic.anonymize_url(self.elastic.index_url)))

        enriched_items = [eitem for eitem in enrich_backend.fetch()]
        enriched_items = sorted(enriched_items, key=lambda k: k["commit_date"])
        elastic_out = ElasticSearch(enrich_backend.elastic.url, out_index)

        # EVOLUTION: No Incremental
        evolution_items = []

        # origin -> file -> details
        cache_dict = dict()
        metrics = ["ccn", "num_funs", "tokens", "loc", "comments", "blanks"]
        num_items = 0
        ins_items = 0

        for eitem in enriched_items:
            evolution_item = {
                "id": eitem['id'],
                "commit_sha": eitem["commit_sha"],
                "origin": eitem["origin"],
                "file_path": eitem["file_path"],
                "modules": eitem["modules"],
                "author_date": eitem["author_date"],
                "commit_date": eitem["commit_date"],
                "metadata__updated_on": eitem["metadata__updated_on"]
            }
            file_level_detail = {}

            for metric in metrics:
                if eitem[metric] is not None:
                    file_level_detail[metric] = eitem[metric]
                else:
                    file_level_detail[metric] = 0

            # Update cache Dict
            if cache_dict.get(eitem["origin"], False):
                # origin entry found
                if cache_dict[eitem["origin"]].get(eitem["file_path"], False):
                    # file entry found
                    for metric in metrics:
                        cache_dict[eitem["origin"]]["total_" + metric] += \
                            (file_level_detail[metric] - cache_dict[eitem["origin"]][eitem["file_path"]][metric])
                else:
                    # new file entry
                    for metric in metrics:
                        cache_dict[eitem["origin"]]["total_" + metric] += file_level_detail[metric]
            else:
                # new origin
                cache_dict[eitem["origin"]] = {}
                for metric in metrics:
                    cache_dict[eitem["origin"]]["total_" + metric] = file_level_detail[metric]

            for metric in metrics:
                evolution_item["total_" + metric] = cache_dict[eitem["origin"]]["total_" + metric]

            if eitem["loc"] is None:
                # Deleted file entry
                cache_dict[eitem["origin"]].pop(eitem["file_path"], None)
                for metric in metrics:
                    file_level_detail[metric] = None
            else:
                cache_dict[eitem["origin"]][eitem["file_path"]] = file_level_detail

            evolution_item.update(file_level_detail)

            if cache_dict[eitem["origin"]]["total_loc"]:
                total_lines = cache_dict[eitem["origin"]]["total_loc"] + cache_dict[eitem["origin"]]["total_comments"] + \
                    cache_dict[eitem["origin"]]["total_blanks"]
                evolution_item["total_comments_ratio"] = cache_dict[eitem["origin"]]["total_comments"] / total_lines
                evolution_item["total_blanks_ratio"] = cache_dict[eitem["origin"]]["total_blanks"] / total_lines
            else:
                evolution_item["total_comments_ratio"] = cache_dict[eitem["origin"]]["total_comments"]
                evolution_item["total_blanks_ratio"] = cache_dict[eitem["origin"]]["total_blanks"]

            evolution_item["comments_ratio"] = eitem["comments_ratio"]
            evolution_item["blanks_ratio"] = eitem["blanks_ratio"]

            evolution_items.append(evolution_item)
            # PUSH
            if len(evolution_items) >= self.elastic.max_items_bulk:
                num_items += len(evolution_items)
                ins_items += elastic_out.bulk_upload(evolution_items, self.get_field_unique_id())
                evolution_items = []

        if len(evolution_items) > 0:
            num_items += len(evolution_items)
            ins_items += elastic_out.bulk_upload(evolution_items, self.get_field_unique_id())

        if num_items != ins_items:
            missing = num_items - ins_items
            logger.error("%s/%s missing items for Study Enricher", str(missing), str(num_items))
        else:
            logger.info("%s items inserted for Study Enricher", str(num_items))

    def __fix_field_date(self, date_value):
        """Fix possible errors in the field date"""

        field_date = str_to_datetime(date_value)

        try:
            _ = int(field_date.strftime("%z")[0:3])
        except ValueError:
            field_date = field_date.replace(tzinfo=None)

        return field_date.isoformat()
