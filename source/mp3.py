from customer import Customer

import psycopg2

from config import read_config
from messages import *

POSTGRESQL_CONFIG_FILE_NAME = "database.cfg"

"""
    Connects to PostgreSQL database and returns connection object.
"""


def connect_to_db():
    db_conn_params = read_config(filename=POSTGRESQL_CONFIG_FILE_NAME, section="postgresql")
    conn = psycopg2.connect(**db_conn_params)
    conn.autocommit = False
    return conn


"""
    Splits given command string by spaces and trims each token.
    Returns token list.
"""


def tokenize_command(command):
    tokens = command.split(" ")
    return [t.strip() for t in tokens]


"""
    Prints list of available commands of the software.
"""


def help():
    # prints the choices for commands and parameters
    print("\n*** Please enter one of the following commands ***")
    print("> help")
    print("> sign_up <email> <password> <first_name> <last_name> <plan_id>")
    print("> sign_in <email> <password>")
    print("> sign_out")
    print("> show_plans")
    print("> show_subscription")
    print("> subscribe <plan_id>")
    print("> watched_movies <movie_id_1> <movie_id_2> <movie_id_3> ... <movie_id_n>")
    print("> search_for_movies <keyword_1> <keyword_2> <keyword_3> ... <keyword_n>")
    print("> suggest_movies")
    print("> quit")


"""
    Saves customer with given details.
    - Return type is a tuple, 1st element is a boolean and 2nd element is the response message from messages.py.
    - If the operation is successful, commit changes and return tuple (True, CMD_EXECUTION_SUCCESS).
    - If any exception occurs; rollback, do nothing on the database and return tuple (False, CMD_EXECUTION_FAILED).
"""


def sign_up(conn, email, password, first_name, last_name, plan_id):
    #cursor
    cur = conn.cursor()

    try:
        cur.execute("INSERT INTO Customer(email,password,first_name,last_name,session_count,plan_id) VALUES (%s,%s,%s,%s,0,%s)",
        (email,password,first_name,last_name,plan_id))
            
        conn.commit()
        cur.close()

        return True, CMD_EXECUTION_SUCCESS 

    except:
        conn.rollback()
        return False, CMD_EXECUTION_FAILED




"""
    Retrieves customer information if email and password is correct and customer's session_count < max_parallel_sessions.
    - Return type is a tuple, 1st element is a customer object and 2nd element is the response message from messages.py.
    - If email or password is wrong, return tuple (None, USER_SIGNIN_FAILED).
    - If session_count < max_parallel_sessions, commit changes (increment session_count) and return tuple (customer, CMD_EXECUTION_SUCCESS).
    - If session_count >= max_parallel_sessions, return tuple (None, USER_ALL_SESSIONS_ARE_USED).
    - If any exception occurs; rollback, do nothing on the database and return tuple (None, USER_SIGNIN_FAILED).
"""


def sign_in(conn, email, password):
    #cursor
    cur = conn.cursor()

    #execute query
    try:
        cur.execute("SELECT * FROM Customer WHERE email = %s and password = %s",(email,password))
        query = cur.fetchone()
    except:
        return None, USER_SIGNIN_FAILED

    #get attributs

    if query == None: # if user is not found
        return None, USER_SIGNIN_FAILED
    else: # if user is found
        try:
            customer_id = query[0]
            email = query[1]
            name = query[3]
            surname = query[4]
            current_session_count = query[5]
            plan_id = query[6]
        except:
            return None, USER_SIGNIN_FAILED

        #execute query
        try:
            cur.execute("SELECT * FROM Plan WHERE plan_id = %s",[plan_id])
            query = cur.fetchone()
        except:
            return None, USER_SIGNIN_FAILED

        #get attributs
        try:
            max_parallel_sessions = query[3]
        except:
            return None, USER_SIGNIN_FAILED

        if(current_session_count < max_parallel_sessions):
            #create customer object
            customer = Customer(customer_id = customer_id, email = email, first_name = name , last_name = surname,session_count = current_session_count+1,plan_id = plan_id)
            try:
                cur.execute("UPDATE Customer SET session_count = %s WHERE customer_id = %s",(current_session_count+1,customer_id))
                conn.commit()
                cur.close()
                return customer,CMD_EXECUTION_SUCCESS
            except:
                conn.rollback()
                return None, USER_SIGNIN_FAILED
            
        else:
            return None, USER_ALL_SESSIONS_ARE_USED


"""
    Signs out from given customer's account.
    - Return type is a tuple, 1st element is a boolean and 2nd element is the response message from messages.py.
    - Decrement session_count of the customer in the database.
    - If the operation is successful, commit changes and return tuple (True, CMD_EXECUTION_SUCCESS).
    - If any exception occurs; rollback, do nothing on the database and return tuple (False, CMD_EXECUTION_FAILED).
"""


def sign_out(conn, customer):
    #cursor
    cur = conn.cursor()
    if(customer.session_count > 0):
        try:
            cur.execute("UPDATE Customer SET session_count = %s WHERE customer_id = %s",(customer.session_count - 1, customer.customer_id))
            conn.commit()
            cur.close()
            return True, CMD_EXECUTION_SUCCESS
        except:
            conn.rollback()
            return False, CMD_EXECUTION_FAILED
    else:
        return False, CMD_EXECUTION_FAILED


"""
    Quits from program.
    - Return type is a tuple, 1st element is a boolean and 2nd element is the response message from messages.py.
    - Remember to sign authenticated user out first.
    - If the operation is successful, commit changes and return tuple (True, CMD_EXECUTION_SUCCESS).
    - If any exception occurs; rollback, do nothing on the database and return tuple (False, CMD_EXECUTION_FAILED).
"""


def quit(conn, customer):
    # TODO: Implement this function
    return False, CMD_EXECUTION_FAILED


"""
    Retrieves all available plans and prints them.
    - Return type is a tuple, 1st element is a boolean and 2nd element is the response message from messages.py.
    - If the operation is successful; print available plans and return tuple (True, CMD_EXECUTION_SUCCESS).
    - If any exception occurs; return tuple (False, CMD_EXECUTION_FAILED).

    Output should be like:
    #|Name|Resolution|Max Sessions|Monthly Fee
    1|Basic|720P|2|30
    2|Advanced|1080P|4|50
    3|Premium|4K|10|90
"""


def show_plans(conn):
    #cursor
    cur = conn.cursor()
    
    try:
        sql = ("SELECT * FROM Plan")
        cur.execute(sql)
        query = query = cur.fetchall()
        cur.close()

        print("#|Name|Resolution|Max Sessions|Monthly Fee")
        for row in query:
            print(str(row[0])+ "|" + str(row[1]) + "|" + str(row[2]) + "|" + str(row[3]) + "|" + str(row[4]))        
        return True, CMD_EXECUTION_SUCCESS

    except:
        return False, CMD_EXECUTION_FAILED
    

"""
    Retrieves authenticated user's plan and prints it. 
    - Return type is a tuple, 1st element is a boolean and 2nd element is the response message from messages.py.
    - If the operation is successful; print the authenticated customer's plan and return tuple (True, CMD_EXECUTION_SUCCESS).
    - If any exception occurs; return tuple (False, CMD_EXECUTION_FAILED).

    Output should be like:
    #|Name|Resolution|Max Sessions|Monthly Fee
    1|Basic|720P|2|30
"""


def show_subscription(conn, customer):
    #cursor
    cur = conn.cursor()
    try:
        cur.execute("SELECT * FROM Plan WHERE plan_id = %s", [customer.plan_id])
        query = cur.fetchone()

        print("#|Name|Resolution|Max Sessions|Monthly Fee")
        print(str(query[0])+ "|" + str(query[1]) + "|" + str(query[2]) + "|" + str(query[3]) + "|" + str(query[4]))

        cur.close()

        return True, CMD_EXECUTION_SUCCESS
    except:
        return False, CMD_EXECUTION_FAILED
    

"""
    Insert customer-movie relationships to Watched table if not exists in Watched table.
    - Return type is a tuple, 1st element is a boolean and 2nd element is the response message from messages.py.
    - If a customer-movie relationship already exists, do nothing on the database and return (True, CMD_EXECUTION_SUCCESS).
    - If the operation is successful, commit changes and return tuple (True, CMD_EXECUTION_SUCCESS).
    - If any one of the movie ids is incorrect; rollback, do nothing on the database and return tuple (False, CMD_EXECUTION_FAILED).
    - If any exception occurs; rollback, do nothing on the database and return tuple (False, CMD_EXECUTION_FAILED).
"""


def watched_movies(conn, customer, movie_ids):
    #cursor
    cur = conn.cursor()
    try:
        for movie_id in movie_ids:
            cur.execute("SELECT * FROM Movies WHERE movie_id = %s",(movie_id,))
            query = cur.fetchone()
            if query == None: # check one of the movie ids is incorrect or not. If it is , rollback and return
                conn.rollback()
                return False, CMD_EXECUTION_FAILED
    except:
        conn.rollback()
        return False, CMD_EXECUTION_FAILED

    new_movie_ids = list()
    #compare given movie_ids with already in database. If the given movie id does not exists in database, add it to new_movie_ids list
    try:
        for movie_id in movie_ids: 
            cur.execute("SELECT * FROM Watched WHERE customer_id = %s and movie_id = %s",(customer.customer_id,movie_id))
            query = cur.fetchone()
            if query == None:
                new_movie_ids.append(movie_id)
    except:
        conn.rollback()
        return False, CMD_EXECUTION_FAILED
    
    if not new_movie_ids: # all movies are already watched do nothing and return success message
        conn.rollback()
        return True, CMD_EXECUTION_SUCCESS

    try:
        for new_movie_id in new_movie_ids:
            cur.execute("INSERT INTO Watched VALUES (%s,%s)",(customer.customer_id,new_movie_id))
    except:
        conn.rollback()
        return False, CMD_EXECUTION_FAILED
    
    conn.commit()
    return True, CMD_EXECUTION_SUCCESS


"""
    Subscribe authenticated customer to new plan.
    - Return type is a tuple, 1st element is a customer object and 2nd element is the response message from messages.py.
    - If target plan does not exist on the database, return tuple (None, SUBSCRIBE_PLAN_NOT_FOUND).
    - If the new plan's max_parallel_sessions < current plan's max_parallel_sessions, return tuple (None, SUBSCRIBE_MAX_PARALLEL_SESSIONS_UNAVAILABLE).
    - If the operation is successful, commit changes and return tuple (customer, CMD_EXECUTION_SUCCESS).
    - If any exception occurs; rollback, do nothing on the database and return tuple (None, CMD_EXECUTION_FAILED).
"""


def subscribe(conn, customer, plan_id):
    #cursor
    cur = conn.cursor()

    try: 
        cur.execute("SELECT * FROM Plan WHERE plan_id= %s",[customer.plan_id])
        query = cur.fetchone()
        old_max_parallel_sessions = query[3]
    except: 
        conn.rollback()
        return None, CMD_EXECUTION_FAILED
    
    try: 
        cur.execute("SELECT * FROM Plan WHERE plan_id= %s",[plan_id])
        query = cur.fetchone()
        new_max_parallel_sessions = query[3]
    except: 
        conn.rollback()
        return None, SUBSCRIBE_PLAN_NOT_FOUND

    if(old_max_parallel_sessions <= new_max_parallel_sessions):
        customer = Customer(customer_id = customer.customer_id, email = customer.email, 
        first_name = customer.first_name, last_name = customer.last_name, session_count = customer.session_count,plan_id = plan_id)
        try:
            cur.execute("UPDATE Customer SET plan_id = %s WHERE customer_id = %s",(plan_id, customer.customer_id))
            conn.commit()
            cur.close()
            return customer, CMD_EXECUTION_SUCCESS
        except:
            conn.rollback()
            return None, CMD_EXECUTION_FAILED
    else:
        return None, SUBSCRIBE_MAX_PARALLEL_SESSIONS_UNAVAILABLE

"""
    Searches for movies with given search_text.
    - Return type is a tuple, 1st element is a boolean and 2nd element is the response message from messages.py.
    - Print all movies whose titles contain given search_text IN CASE-INSENSITIVE MANNER.
    - If the operation is successful; print movies found and return tuple (True, CMD_EXECUTION_SUCCESS).
    - If any exception occurs; return tuple (False, CMD_EXECUTION_FAILED).

    Output should be like:
    Id|Title|Year|Rating|Votes|Watched
    "tt0147505"|"Sinbad: The Battle of the Dark Knights"|1998|2.2|149|0
    "tt0468569"|"The Dark Knight"|2008|9.0|2021237|1
    "tt1345836"|"The Dark Knight Rises"|2012|8.4|1362116|0
    "tt3153806"|"Masterpiece: Frank Millers The Dark Knight Returns"|2013|7.8|28|0
    "tt4430982"|"Batman: The Dark Knight Beyond"|0|0.0|0|0
    "tt4494606"|"The Dark Knight: Not So Serious"|2009|0.0|0|0
    "tt4498364"|"The Dark Knight: Knightfall - Part One"|2014|0.0|0|0
    "tt4504426"|"The Dark Knight: Knightfall - Part Two"|2014|0.0|0|0
    "tt4504908"|"The Dark Knight: Knightfall - Part Three"|2014|0.0|0|0
    "tt4653714"|"The Dark Knight Falls"|2015|5.4|8|0
    "tt6274696"|"The Dark Knight Returns: An Epic Fan Film"|2016|6.7|38|0
"""


def search_for_movies(conn, customer, search_text):
    #cursor
    cur = conn.cursor()
    try:
        cur.execute("SELECT * FROM Movies m LEFT JOIN Watched w ON m.movie_id = w.movie_id WHERE title ILIKE %s ORDER BY m.movie_id", 
        ["%"+search_text + "%"])
        query = cur.fetchall()
    except:
        conn.rollback()
        return False, CMD_EXECUTION_FAILED

    print("Id|Title|Year|Rating|Votes|Watched")
    for row in query:
        if(row[5] == customer.customer_id):
            print(str(row[0]) + "|" + str(row[1]) + "|" + str(row[2]) + "|" + str(row[3]) + "|" + str(row[4]) + "|" + str(1))
        elif(row[5] == None): 
            print(str(row[0]) + "|" + str(row[1]) + "|" + str(row[2]) + "|" + str(row[3]) + "|" + str(row[4]) + "|" + str(0)) 
        else:
            continue
    return True, CMD_EXECUTION_SUCCESS
"""
    Suggests combination of these movies:
        1- Find customer's genres. For each genre, find movies with most number of votes among the movies that the customer didn't watch.

        2- Find top 10 movies with most number of votes and highest rating, such that these movies are released 
           after 2010 ( [2010, today) ) and the customer didn't watch these movies.
           (descending order for votes, descending order for rating)

        3- Find top 10 movies with votes higher than the average number of votes of movies that the customer watched.
           Disregard the movies that the customer didn't watch.
           (descending order for votes)

    - Return type is a tuple, 1st element is a boolean and 2nd element is the response message from messages.py.    
    - Output format and return format are same with search_for_movies.
    - Order these movies by their movie id, in ascending order at the end.
    - If the operation is successful; print movies suggested and return tuple (True, CMD_EXECUTION_SUCCESS).
    - If any exception occurs; return tuple (False, CMD_EXECUTION_FAILED).
"""


def suggest_movies(conn, customer):
    #cursor
    cur = conn.cursor()
    sql =   """
select *
from
((SELECT DISTINCT m.movie_id,
                m.title,
                m.movie_year,
                m.rating,
                m.votes
FROM   movies m,
       genres g,
       (SELECT g.genre_name,
               Max(not_watched.votes) AS max_votes
        FROM   genres g,
               (SELECT *
                FROM   movies m
                WHERE  m.movie_id NOT IN (SELECT w.movie_id
                                          FROM   watched w
                                          WHERE  w.customer_id = %s)) AS
               not_watched,
               (SELECT DISTINCT g.genre_name AS genre_name
                FROM   watched w,
                       genres g
                WHERE  w.movie_id = g.movie_id
                       AND w.customer_id = %s) AS watched_genres
        WHERE  not_watched.movie_id = g.movie_id
               AND g.genre_name = watched_genres.genre_name
        GROUP  BY g.genre_name) AS notwatched_maxvotes
WHERE  m.movie_id = g.movie_id
       AND g.genre_name = notwatched_maxvotes.genre_name
       AND m.votes = notwatched_maxvotes.max_votes)  
union 
(SELECT *
FROM   movies m
WHERE  m.movie_year >= '2010'
       AND m.movie_id NOT IN (SELECT w.movie_id
                              FROM   watched w
                              WHERE  w.customer_id = %s)
ORDER  BY m.votes DESC,
          m.votes DESC
LIMIT  10)  
union 
(SELECT m.movie_id,
       m.title,
       m.movie_year,
       m.rating,
       m.votes
FROM   movies m
WHERE  m.movie_id NOT IN (SELECT w.movie_id
                          FROM   watched w
                          WHERE  w.customer_id = %s)
       AND m.votes > (SELECT Avg(m.votes)
                      FROM   movies m,
                             watched w
                      WHERE  m.movie_id = w.movie_id
                             AND w.customer_id = %s)
ORDER  BY m.votes desc)) as t
order by t.movie_id asc """

    try:
        cur.execute(sql,(customer.customer_id,customer.customer_id,customer.customer_id,customer.customer_id,customer.customer_id))
        query = cur.fetchall()
    except:
        return False, CMD_EXECUTION_FAILED

    print("Id|Title|Year|Rating|Votes")
    for row in query:
        print(str(row[0]) + "|" + str(row[1]) + "|" + str(row[2]) + "|" + str(row[3]) + "|" + str(row[4]))

    return True, CMD_EXECUTION_SUCCESS