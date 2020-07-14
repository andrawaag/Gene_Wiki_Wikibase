from wikidataintegrator import wdi_core, wdi_login
import os
from rdflib import Graph, URIRef
import pandas as pd
from wikidataintegrator.wdi_helpers import try_write
from datetime import datetime
import copy
import pprint


def createDOReference(doid):
    statedin = wdi_core.WDItemID("Q5282129", prop_nr="P248", is_reference=True)
    retrieved = datetime.now()
    timeStringNow = retrieved.strftime("+%Y-%m-%dT00:00:00Z")
    refRetrieved = wdi_core.WDTime(timeStringNow, prop_nr="P813", is_reference=True)
    doid = wdi_core.WDExternalID(doid, prop_nr="P699", is_reference=True)
    return [statedin, refRetrieved, doid]


print("Logging in...")
if "WDUSER" in os.environ and "WDPASS" in os.environ:
    WDUSER = os.environ['WDUSER']
    WDPASS = os.environ['WDPASS']
else:
    raise ValueError("WDUSER and WDPASS must be specified in local.py or as environment variables")

wikibase = "https://www.wikidata.org/w/api.php"
login = wdi_login.WDLogin(WDUSER, WDPASS,mediawiki_api_url=wikibase)

## The following section is to download the orginal Disease ontology
print("\nDownloading the Disease Ontology...")
url = "https://raw.githubusercontent.com/DiseaseOntology/HumanDiseaseOntology/master/src/ontology/releases/2020-06-18/doid.owl"

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
  SELECT * WHERE {?item wdt:P2888 ?exactMatch ; wdt:P699 ?doid .}
"""
existing_disease = dict()
results = wdi_core.WDItemEngine.execute_sparql_query(query=query)

for result in results["results"]["bindings"]:
    existing_disease[str(result["exactMatch"]["value"])] = result["item"]["value"]

for index, row in df_doNative.iterrows():

    # Remove old statements
    #old data
    doid = row["doid"]
    old_data_query = f"SELECT ?disease ?p (COUNT(?reference) AS ?references) WHERE \u007b ?disease wdt:P699 '{doid}' ; ?p ?node . ?node prov:wasDerivedFrom ?reference . ?reference pr:P699 '{doid}' .\u007d GROUP BY ?disease ?doid ?p "
    results = wdi_core.WDItemEngine.execute_sparql_query(old_data_query)
    pprint.pprint(results)
    print(old_data_query)
    delete_data = []
    for result in results["results"]["bindings"]:
        if result["references"]["value"] == 1:
            print(result["p"]["value"])
            delete_data.append(wdi_core.WDBaseDataType.delete_statement(prop_nr=result["p"]["value"].replace("http://www.wikidata.org/prop/", "")))
    qid = existing_disease[row["do_uri"]].replace("http://www.wikidata.org/entity/", "")
    wb_olddo_item = wdi_core.WDItemEngine(wd_item_id=qid, data=delete_data, keep_good_ref_statements=True)
    try_write(wb_olddo_item, record_id=row["doid"], record_prop="P699", edit_summary="cleaned references a Disease Ontology",
              login=login)


    # Add recent disease ontology statements
    data = []
    do_reference = createDOReference(row["doid"])
    # disease ontology ID
    data.append(wdi_core.WDExternalID(row["doid"], prop_nr="P699", references=[copy.deepcopy(do_reference)]))

    # disease ontology URI
    data.append(wdi_core.WDUrl(row["do_uri"], prop_nr="P2888", references=[copy.deepcopy(do_reference)]))

    # identifiers.org URI
    data.append(wdi_core.WDUrl("http://identifiers.org/doid/"+row["doid"], prop_nr="P2888", references=[copy.deepcopy(do_reference)]))

    # MESH
    if "MESH:" in row["exactMatch"]:
        data.append(wdi_core.WDExternalID(row["exactMatch"], prop_nr="P486", references=[copy.deepcopy(do_reference)]))

    if str(row["do_uri"]) in existing_disease.keys():
        qid = existing_disease[row["do_uri"]].replace("http://www.wikidata.org/entity/", "")
        wb_do_item = wdi_core.WDItemEngine(wd_item_id=qid, data=data, keep_good_ref_statements=True)
    else:
        wb_do_item = wdi_core.WDItemEngine(data=data, keep_good_ref_statements=True)

    wb_do_item.set_label(row["label"], lang="en")
    wb_do_item.set_description("human disease", lang="en")

    if row["aliases"] != "":
        wb_do_item.set_aliases(row["aliases"].split("|"), lang="en")
    try_write(wb_do_item, record_id=row["doid"], record_prop="P699", edit_summary="Updated a Disease Ontology",
              login=login)
    sys.exit()