"""COMP 3705 Python Project
Movie recommender by Sophie Smithgraham
Gets IMdB ids, makes requests to OMdB, handles transactions to Neo4j """
import pprint
import requests
import pandas as pd

def get_ids():
    """The IMdB ids require leading 0s that need to be added in depending on length"""
    orig_ids = []
    fixed_ids = []
    id_file = pd.read_csv('links.csv')
    orig_ids = id_file["imdbId"].values.tolist()
    for index in range(0, len(orig_ids)):
        if len(str(orig_ids[index])) <= 5:
            id_with_zero = "00" + str(orig_ids[index])
            fixed_ids.append(id_with_zero)
        elif len(str(orig_ids[index])) > 5 and len(str(orig_ids[index])) < 8:
            id_with_zero = "0" + str(orig_ids[index])
            fixed_ids.append(id_with_zero)
        else:
            fixed_ids.append(orig_ids[index])
    return fixed_ids

def request_movies(minim, maxim, ids):
    """Sends requests to the OMdB API and saves the response."""
    data = []
    for i, imdb_id in enumerate(ids):
        if minim <= i < maxim:
            link = "http://www.omdbapi.com/?i=tt" + str(imdb_id) + "&apikey=13f2e9d5"
            response = requests.get(link)
            if response.ok: #only appends if successfully retrieves data from omdb
                data.append(response.json())
            else:
                print('skip')
    return data

def get_names_list(unsplit_list):
    """Seperating names of actors, directors, writers into seperate strings"""
    #names_list = []
    cleaned_list = str(unsplit_list).split(",")
    return cleaned_list

def add_movie(driver, title, rel, plot, run, act, writ, direct, lang, rated, gen):
    """Creates session for adding nodes to graph"""
    with driver.session() as session:
        return session.execute_write(create_movie, title, rel, plot, run,\
        act, writ, direct, lang, rated, gen)

def create_movie(tx, title, rel, plot, run, act, writ, direct, lang, rated, gen):
    """Sends query to Neo4j. Gets a cleaner list of names for certain arguments"""
    actors = get_names_list(act)
    directors = get_names_list(direct)
    writers = get_names_list(writ)
    genres = get_names_list(gen)
    languages = get_names_list(lang)

    tx.run("MERGE(m:Movie {title: $title, released: $rel, plot:$plot, runtime: $run})\
     MERGE(r:Rated{rated:$rated}) MERGE(m)-[:RATED]->(r) RETURN m",\
     title=title, rel=rel, plot=plot, run=run, lang=lang, rated=rated, gen=gen)
    for name in actors: #Actor nodes
        tx.run("MATCH (x:Movie{title:$title}) MERGE(a:Person:Actor\
         {name:'" + name.replace("'", '').strip() + "'}) MERGE(a)-[:ACTED_IN]->(x)", title=title)
    for name in directors: #Director nodes
        tx.run("MATCH (x:Movie{title:$title}) MERGE(d:Person:Director\
         {name:'" + name.replace("'", '').strip() + "'}) MERGE(d)-[:DIRECTED]->(x)", title=title)
    for name in writers: #Writer nodes
        tx.run("MATCH (x:Movie{title:$title}) MERGE(w:Person:Writer\
         {name:'" + name.replace("'", '').strip() + "'}) MERGE(w)-[:WROTE_FOR]->(x)", title=title)
    for name in genres: #Genre nodes
        tx.run("MATCH (x:Movie{title:$title}) MERGE(g:Genre\
         {genre:'" + name.strip() + "'}) MERGE(x)-[:CATEGORIZED_AS]->(g)", title=title)
    for name in languages: #Language nodes
        tx.run("MATCH (x:Movie{title:$title}) MERGE(l:Language\
        {language:'" + name.replace("'", '').strip() + "' })\
        MERGE(x)-[:LANGUAGE]->(l)", title=title)
    print("Added: " + title)

def get_recs(driver, title):
    """Creates session for inputting the Node Similarity Algorithm"""
    with driver.session() as session:
        return session.execute_write(create_recs, title)

def create_recs(tx, title):
    """Sends query for Node Similarity Algorithm"""
    results = tx.run("CALL gds.nodeSimilarity.stream('movies')\
     YIELD node1, node2, similarity WHERE gds.util.asNode(node1).title = $title\
     RETURN gds.util.asNode(node2).title AS Movie, gds.util.asNode(node2).plot AS Plot, similarity\
     ORDER BY similarity DESCENDING, Movie LIMIT 5", title=title)
    prepri = pprint.PrettyPrinter(indent=4) #Prints results
    clean_results = [dict(i) for i in results]
    if len(clean_results) == 0:
        print("Movie not found!")
    else:
        prepri.pprint(clean_results)
    return clean_results

def create_projection(tx):
    """Sends query for creating the graph projection"""
    return tx.run("CALL gds.graph.project('movies',\
    ['Movie','Person','Genre','Language','Rated'],['CATEGORIZED_AS','LANGUAGE','RATED',\
    {ACTED_IN:{orientation:'REVERSE'},DIRECTED:{orientation:'REVERSE'},\
    WROTE_FOR:{orientation:'REVERSE'}}])")

def get_projection(driver):
    """Creates session for graph projection"""
    with driver.session() as session:
        return session.execute_write(create_projection)

def delete_projection(tx):
    """Query for removing graph projection"""
    return tx.run("CALL gds.graph.drop('movies', false) YIELD graphName")

def get_delete_projection(driver):
    """Creates session for removing graph projection"""
    with driver.session() as session:
        return session.execute_write(delete_projection)

def add_to_neo4j(driver, mini, maxi):
    """Gets data from OMdB, then goes through the data received to create the nodes in Neo4j.
    Resets the graph projection"""
    ids = get_ids()
    get_delete_projection(driver)
    movie_data = request_movies(mini, maxi, ids)
    try:
        datafr = pd.DataFrame(movie_data)
        columns = datafr[["Title", "Released", "Plot", "Runtime", "Actors",\
        "Writer", "Director", "Language", "Rated", "Genre"]]
        movies = columns.values.tolist()
        for movie in range(0, (len(movies))):
            add_movie(driver, movies[movie][0], movies[movie][1],\
            movies[movie][2], movies[movie][3], movies[movie][4], movies[movie][5],\
            movies[movie][6], movies[movie][7], movies[movie][8], movies[movie][9])
    except KeyError:
        print("Request limit reached, couldn't receive movie data!")
    finally:
        get_projection(driver)
