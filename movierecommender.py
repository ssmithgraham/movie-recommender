"""COMP 3705 Python Project
Movie recommender by Sophie Smithgraham
For the user to add nodes and run the algorithm"""
from neo4j import GraphDatabase, basic_auth
import getdata

def front_end():
    """Gets starting and end point for which movies should be requested and added.
    Asks for a movie title to find similar movie"""
    driver = GraphDatabase.driver("bolt://3.234.254.82:7687"\
    , auth=basic_auth("neo4j", "things-conn-tents"))
    add_nodes = input("add nodes to database? (y/n)")
    if add_nodes == 'y':
        min_id = input("enter min:")
        max_id = input("enter max:")
        print("Requesting movies...")
        getdata.add_to_neo4j(driver, int(min_id), int(max_id))
    orig_title = input("Enter movie title:")
    print("Getting recommendations...")
    getdata.get_recs(driver, orig_title)
    driver.close()

front_end()
