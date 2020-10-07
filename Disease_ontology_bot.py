from wikidataintegrator import wdi_core, wdi_login
import os
from rdflib import Graph, URIRef
import pandas as pd
from wikidataintegrator.wdi_helpers import try_write
from datetime import datetime
import copy


def createDOReference(doid):
    statedin = wdi_core.WDItemID("Q1", prop_nr="P6", is_reference=True)
    retrieved = datetime.now()
    timeStringNow = retrieved.strftime("+%Y-%m-%dT00:00:00Z")
    refRetrieved = wdi_core.WDTime(timeStringNow, prop_nr="P7", is_reference=True)
    doid = wdi_core.WDExternalID(doid, prop_nr="P4", is_reference=True)
    return [statedin, refRetrieved, doid]


# This code is to Login to Wikibase. The pattern of api is the URL of the wikibase + /w/api.php
# In this example that is https://do-mondo.semscape.org/w/api.php

print("Logging in...")
if "WDUSER" in os.environ and "WDPASS" in os.environ:
    WDUSER = os.environ['WDUSER']
    WDPASS = os.environ['WDPASS']
else:
    raise ValueError("WDUSER and WDPASS must be specified in local.py or as environment variables")

wikibase = "https://do-mondo.semscape.org/w/api.php"
login = wdi_login.WDLogin(WDUSER, WDPASS,mediawiki_api_url=wikibase)

## The following section is to download the orginal Disease ontology
print("\nDownloading the Disease Ontology...")
url = "https://raw.githubusercontent.com/DiseaseOntology/HumanDiseaseOntology/main/src/ontology/releases/2020-09-28/doid.owl"

doGraph = Graph()
doGraph.parse(url, format="xml")

df_doNative = pd.DataFrame(columns=["do_uri", "doid", "label", "subclassof", "aliases", "exactMatch"])

qres = doGraph.query(
    """
       PREFIX obo: <http://www.geneontology.org/formats/oboInOwl#>
       PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
       PREFIX oboInOwl: <http://www.geneontology.org/formats/oboInOwl#>

       SELECT DISTINCT ?do_uri ?doid ?label (GROUP_CONCAT(?subClassOf;separator="|") as ?subclasses) (GROUP_CONCAT(?exactsynonym;separator="|") as ?exact_synonyms)
                       ?exactMatch 
       WHERE {
          ?do_uri obo:id ?doid ;
                  rdfs:label ?label .
         OPTIONAL {?do_uri rdfs:subClassOf ?subClassOf ;}
         OPTIONAL {?do_uri oboInOwl:hasExactSynonym ?exactsynonym}
         
         OPTIONAL {?s <http://www.w3.org/2004/02/skos/core#exactMatch> ?o ;
                      <http://www.w3.org/2002/07/owl#annotatedSource> ?do_uri ;
                      <http://www.w3.org/2002/07/owl#annotatedTarget> ?exactMatch .}

       }
       GROUP BY ?do_uri """)

for row in qres:
    df_doNative = df_doNative.append({
     "do_uri": str(row[0]),
     "doid": str(row[1]),
     "label":  str(row[2]),
     "subclassof": str(row[3]),
     "aliases": str(row[4]),
     "exactMatch": str(row[5])
      }, ignore_index=True)

query = """
  PREFIX wbt: <http://do-mondo.semscape.org/prop/direct/>
  SELECT * WHERE {?item wbt:P3 ?exactMatch}
"""
existing_disease = dict()
results = wdi_core.WDItemEngine.execute_sparql_query(query=query, endpoint="https://do-mondo.semscape.org/query/sparql" )

for result in results["results"]["bindings"]:
    existing_disease[str(result["exactMatch"]["value"])] = result["item"]["value"]

for index, row in df_doNative.iterrows():
    data = []
    do_reference = createDOReference(row["doid"])
    # disease ontology ID
    data.append(wdi_core.WDExternalID(row["doid"], prop_nr="P4", references=[copy.deepcopy(do_reference)]))

    # disease ontology URI
    data.append(wdi_core.WDUrl(row["do_uri"], prop_nr="P3", references=[copy.deepcopy(do_reference)]))

    # identifiers.org URI
    data.append(wdi_core.WDUrl("http://identifiers.org/doid/"+row["doid"], prop_nr="P3", references=[copy.deepcopy(do_reference)]))

    # MESH
    if "MESH:" in row["exactMatch"]:
        data.append(wdi_core.WDExternalID(row["exactMatch"], prop_nr="P8", references=[copy.deepcopy(do_reference)]))

    if str(row["do_uri"]) in existing_disease.keys():
        qid = existing_disease[row["do_uri"]].replace("http://do-mondo.semscape.org/entity/", "")
        wb_do_item = wdi_core.WDItemEngine(wd_item_id=qid, data=data, mediawiki_api_url=wikibase,
                                              sparql_endpoint_url="https://do-mondo.semscape.org/query/sparql",
                                              keep_good_ref_statements=True)
    else:
        wb_do_item = wdi_core.WDItemEngine(data=data, mediawiki_api_url=wikibase,
                                              sparql_endpoint_url="https://do-mondo.semscape.org/query/sparql",
                                              keep_good_ref_statements=True)

    wb_do_item.set_label(row["label"], lang="en")
    wb_do_item.set_description("human disease", lang="en")

    if row["aliases"] != "":
        wb_do_item.set_aliases(row["aliases"].split("|"), lang="en")
    try_write(wb_do_item, record_id=row["doid"], record_prop="P4", edit_summary="Updated a Disease Ontology",
              login=login)