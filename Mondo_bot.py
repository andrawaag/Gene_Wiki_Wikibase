from wikidataintegrator import wdi_core, wdi_login
import os
from rdflib import Graph, URIRef
import pandas as pd
import pprint
from wikidataintegrator.wdi_helpers import try_write
from datetime import datetime
import copy

def createMONDOReference(id):
    statedin = wdi_core.WDItemID("Q2", prop_nr="P6", is_reference=True)
    retrieved = datetime.now()
    timeStringNow = retrieved.strftime("+%Y-%m-%dT00:00:00Z")
    refRetrieved = wdi_core.WDTime(timeStringNow, prop_nr="P7", is_reference=True)
    mondoid = wdi_core.WDExternalID(id, prop_nr="P5", is_reference=True)
    return [statedin, refRetrieved, mondoid]

# This code is to Login to Wikibase. The pattern of api is the URL of the wikibase + /w/api.php
# In this example that is https://diseases.semscape.org/w/api.php

## The following section is to download the orginal Disease ontology
print("\nDownloading Mondo ...")
url = "http://purl.obolibrary.org/obo/mondo.owl"

doGraph = Graph()
doGraph.parse(url, format="xml")

df_mondoNative = pd.DataFrame(columns=["mondo_uri", "mondoid", "label", "exactMatches", "aliases"])

qres = doGraph.query(
    """
       PREFIX obo: <http://www.geneontology.org/formats/oboInOwl#>
       PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
       PREFIX oboInOwl: <http://www.geneontology.org/formats/oboInOwl#>

       SELECT DISTINCT ?mondo_uri ?mondoid ?label (GROUP_CONCAT(?exactMatch;separator="|") as ?exactMatches) (GROUP_CONCAT(?exactsynonym;separator="|") as ?exact_synonyms)
       WHERE {
          ?mondo_uri obo:id ?mondoid ;
                  rdfs:label ?label .
         OPTIONAL {?mondo_uri <http://www.w3.org/2004/02/skos/core#exactMatch> ?exactMatch .}
         OPTIONAL {?mondo_uri oboInOwl:hasExactSynonym ?exactsynonym}
       }
       GROUP BY ?mondo_uri """)

for row in qres:
    df_mondoNative = df_mondoNative.append({
     "mondo_uri": str(row[0]),
     "mondoid": str(row[1]),
     "label":  str(row[2]),
     "exactMatches": str(row[3]),
     "aliases": str(row[4])
      }, ignore_index=True)

wikibase = "https://diseases.semscape.org/w/api.php"
sparql = ""
print("Logging in...")
if "WDUSER" in os.environ and "WDPASS" in os.environ:
    WDUSER = os.environ['WDUSER']
    WDPASS = os.environ['WDPASS']
else:
    raise ValueError("WDUSER and WDPASS must be specified in local.py or as environment variables")
login = wdi_login.WDLogin(WDUSER, WDPASS, mediawiki_api_url=wikibase)

query = """
  PREFIX wbt: <http://diseases.semscape.org/prop/direct/>
  SELECT * WHERE {?item wbt:P5 ?exactMatch}
"""
existing_disease = dict()
results = wdi_core.WDItemEngine.execute_sparql_query(query=query, endpoint="https://diseases.semscape.org/query/sparql" )

for result in results["results"]["bindings"]:
    existing_disease[result["exactMatch"]["value"]] = result["item"]["value"]

for index, row in df_mondoNative.iterrows():
    data = []
    mondo_reference = createMONDOReference(row["mondoid"])
    # Mondo ID
    data.append(wdi_core.WDExternalID(row["mondoid"], prop_nr="P5", references=[copy.deepcopy(mondo_reference)]))
    data.append(wdi_core.WDUrl(row["mondo_uri"], prop_nr="P3", references=[copy.deepcopy(mondo_reference)]))

    # exact matches
    for skosExactMatch in list(set(row["exactMatches"].split("|"))):
        if skosExactMatch != "":
            data.append(wdi_core.WDUrl(skosExactMatch, prop_nr="P3", references=[copy.deepcopy(mondo_reference)]))
            # Disease Ontology ID (P4)
            # http://purl.obolibrary.org/obo/DOID_xxxxx
            if "http://purl.obolibrary.org/obo/DOID" in skosExactMatch:
                doid = skosExactMatch.replace("http://purl.obolibrary.org/obo/DOID_", "DOID:")
                data.append(wdi_core.WDExternalID(doid, prop_nr="P4", references=[copy.deepcopy(mondo_reference)]))

            # MeSH descriptor ID (P8)
            # http://identifiers.org/mesh/
            if "http://identifiers.org/mesh/" in skosExactMatch:
                mesh = skosExactMatch.replace("http://identifiers.org/mesh/", "")
                data.append(wdi_core.WDExternalID(mesh, prop_nr="P8", references=[copy.deepcopy(mondo_reference)]))

            # UMLS CUI (P9)
            # http://linkedlifedata.com/resource/umls/id/
            if "http://linkedlifedata.com/resource/umls/id/" in skosExactMatch:
                umls = skosExactMatch.replace("http://linkedlifedata.com/resource/umls/id/", "")
                data.append(wdi_core.WDExternalID(umls, prop_nr="P9", references=[copy.deepcopy(mondo_reference)]))

            # Orphanet ID (P10)
            # http://www.orpha.net/ORDO/Orphanet_
            if "http://www.orpha.net/ORDO/Orphanet_" in skosExactMatch:
                ordo = skosExactMatch.replace("http://www.orpha.net/ORDO/", "")
                data.append(wdi_core.WDExternalID(ordo, prop_nr="P10", references=[copy.deepcopy(mondo_reference)]))

            # NCI Thesaurus ID (P11)
            # http://purl.obolibrary.org/obo/NCIT_
            if "http://purl.obolibrary.org/obo/NCIT_" in skosExactMatch:
                ncit = skosExactMatch.replace("http://purl.obolibrary.org/obo/NCIT_", "NCIT:")
                data.append(wdi_core.WDString(ncit, prop_nr="P11", references=[copy.deepcopy(mondo_reference)]))

            # OMIM (P12)
            # http://identifiers.org/omim/
            if "http://identifiers.org/omim/" in skosExactMatch:
                omim = skosExactMatch.replace("http://identifiers.org/omim/", "")
                data.append(wdi_core.WDExternalID(omim, prop_nr="P12", references=[copy.deepcopy(mondo_reference)]))

    qid = None
    for exactMatch in row["exactMatches"].split("|"):
        if exactMatch in existing_disease.keys():
            qid = existing_disease[exactMatch].replace("http://diseases.semscape.org/entity/", "")
        continue
    if not qid:
        wb_mondo_item = wdi_core.WDItemEngine(data=data, mediawiki_api_url=wikibase,
                                              sparql_endpoint_url="https://diseases.semscape.org/query/sparql",
                                              global_ref_mode="STRICT_KEEP_APPEND",
                                              keep_good_ref_statements=True)
    else:
        wb_mondo_item = wdi_core.WDItemEngine(wd_item_id=qid, data=data, mediawiki_api_url=wikibase,
                                              sparql_endpoint_url="https://diseases.semscape.org/query/sparql",
                                              global_ref_mode="STRICT_KEEP_APPEND",
                                              keep_good_ref_statements=True)

    if wb_mondo_item.get_label() == "":
        wb_mondo_item.set_label(row["label"], lang="en")
    if wb_mondo_item.get_description() == "":
        wb_mondo_item.set_description("human disease", lang="en")
    if row["aliases"] != "":
        print(row["aliases"])
        aliases = wb_mondo_item.get_aliases()
        for new_alias in row["aliases"].split("|"):
            aliases.append(new_alias)
        wb_mondo_item.set_aliases(aliases, lang="en")
    #pprint.pprint(wb_mondo_item.get_wd_json_representation())
    print(wb_mondo_item.wd_item_id)
    #wb_mondo_item.write(login)
    try_write(wb_mondo_item, record_id=row["mondoid"], record_prop="P4", edit_summary="Updated a MondoID", login=login,)