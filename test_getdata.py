"""COMP 3705 Python Project
Movie recommender by Sophie Smithgraham
Test file """

from neo4j import GraphDatabase, basic_auth
import pytest
import getdata

def test_getids():
    """Checks if ids were read from file and added to correctly"""
    returned_ids = getdata.get_ids()
    assert returned_ids[0] == "0114709"

def test_request_movies():
    """Checks if requests were made correctly to the API"""
    returned_data = getdata.request_movies(0, 1, ["0114709"])
    assert returned_data[0].get("Title") == "Toy Story"
    assert returned_data[0].get("Genre") == 'Animation, Adventure, Comedy'

def test_get_names_list():
    """Checks if lists were broken up correctly"""
    returned_list = getdata.get_names_list("Animation, Adventure, Comedy")
    assert returned_list == ['Animation', ' Adventure', ' Comedy']

def test_add_movie():
    """Checks if nodes are added to Neo4j"""
    driver = GraphDatabase.driver("bolt://3.234.254.82:7687"\
    , auth=basic_auth("neo4j", "things-conn-tents"))
    get_delete_movie(driver)
    before = get_find_node(driver)
    assert before == []

    getdata.add_movie(driver, "Toy Story", "22 Nov 1995"\
     , "A cowboy doll is profoundly threatened and jealous when a new spaceman action figure supplants him as top toy in a boy's bedroom."\
     , "81 min", "Tom Hanks, Tim Allen, Don Rickles", "Andrew Stanton, Pete Docter, John Lasseter"\
     , "John Lasseter", "English", "G", "Animation, Adventure, Comedy")
    after = get_find_node(driver)
    assert after[0].get("n").get("title") == "Toy Story"
    driver.close()

def test_get_delete_projection():
    """Checks that graph projection is removed """
    driver = GraphDatabase.driver("bolt://3.234.254.82:7687"\
    , auth=basic_auth("neo4j", "things-conn-tents"))
    getdata.get_delete_projection(driver)
    deleted = get_projections(driver)
    assert deleted == []
    driver.close()

def test_get_projection():
    """Checks that graph projection is created """
    driver = GraphDatabase.driver("bolt://3.234.254.82:7687"\
    , auth=basic_auth("neo4j", "things-conn-tents"))
    getdata.get_projection(driver)
    added = get_projections(driver)
    assert added[0].get("graphName") == "movies"
    driver.close()

def test_get_recs():
    """Checks that algorithm results are expected values """
    driver = GraphDatabase.driver("bolt://3.234.254.82:7687"\
    , auth=basic_auth("neo4j", "things-conn-tents"))

    results = getdata.get_recs(driver, "Toy Story")
    print(results)
    assert results[0].get("Movie") == "Toy Story 2"
    assert results[0].get("similarity") == 0.5263157894736842
    driver.close()

def get_delete_movie(driver):
    with driver.session() as session:
        return session.execute_write(delete_movie)
        
def delete_movie(tx):
    return tx.run("MATCH (n:Movie) WHERE n.title = 'Toy Story' DETACH DELETE n")
    
def get_find_node(driver):
    with driver.session() as session:
        return session.execute_write(find_node)
        
def find_node(tx):
    result = tx.run("MATCH(n:Movie) WHERE n.title = 'Toy Story' RETURN n")
    rlist = [dict(i) for i in result]
    return rlist
    
def list_projection(tx):
    result = tx.run("CALL gds.graph.list() YIELD graphName, nodeCount, relationshipCount\
     RETURN graphName, nodeCount, relationshipCount ORDER BY graphName ASC")
    plist = [dict(i) for i in result]
    return plist
    
def get_projections(driver):
    with driver.session() as session:
        return session.execute_write(list_projection)
