'''
@Author: Subhadeep Bhattacharjee
@Date: 2021-12-23 16:45
@Title: To establish a python-mongodb connection and perform some basic querries.

'''
import pymongo
from pymongo import collection

def show_databases():
    '''
    Description:
        This is used to show all the present databases.
    '''
    print(myclient.list_database_names())

def use_or_create_database(dbname):
    '''
    Description:
        This is used to use an existing database or create a new database.
    Parameter:
        This takes a database name as an input. 
    '''
    db = myclient[dbname]

def show_collections(dbname):
    '''
    Description:
        This function is used to show the collections present in the mongo db specified database.

    Parameter:
        This takes the database and name as an input parameter.
    '''
    db = myclient[dbname]
    print(db.list_collection_names())

def create_collection(dbname, collection_name):
    '''
    Description:
        This function is used to create a collection.
    Parameter:
        This takes a database name and a collection name as an input.
    '''
    db = myclient[dbname]
    mycol = db[collection_name]

def drop_database(dbname):
    '''
    Description:
        This is used to drop a database.
    Parameter:
        This takes the database name as an input.
    '''
    myclient.drop_database(dbname)
    print(f"{dbname} deleted successfully!")

def drop_collection(dbname, collection_name):
    '''
    Description:
        This is used to drop a collection.
    Parameter:
        This takes the database name and collection as an input.
    '''
    db = myclient[dbname]
    db.drop_collection(collection_name)
    print(f"{collection_name} deleted successfully")


def insert_one(dbname,collection_name):
    '''
    Description:
        This is used to insert a single entry into the database.
    '''
    db = myclient[dbname]
    mycol = db[collection_name]
    dictionary = { "name": "John", "address": "Highway 37", "age": 35, "salary": 50000 }

    mycol.insert_one(dictionary)


def insert_many(dbname, collection_name):
    '''
    Description:
        This function is used to insert many documents together in the collection.
    '''
    db = myclient[dbname]
    mycol = db[collection_name]
    dictionary= [
    { "name": "Amy", "address": "Apple st 652", "age": 25, "salary": 27000},
    { "name": "Hannah", "address": "Mountain 21","age": 21, "salary": 15000},
    { "name": "Michael", "address": "Valley 345", "age": 40, "salary": 65000},
    { "name": "Sandy", "address": "Ocean blvd 2", "age": 34, "salary": 65000},
    { "name": "Betty", "address": "Green Grass 1", "age": 34, "salary": 70000},
    { "name": "Richard", "address": "Sky st 331","age": 47, "salary": 100000},
    { "name": "Susan", "address": "One way 98", "age": 50, "salary": 78000},
    { "name": "Vicky", "address": "Yellow Garden 2", "age": 38, "salary": 45000},
    { "name": "Ben", "address": "Park Lane 38", "age": 60, "salary": 120000},
    { "name": "William", "address": "Central st 954", "age": 45, "salary": 78000},
    { "name": "Chuck", "address": "Main Road 989", "age": 18, "salary": 24000},
    { "name": "Viola", "address": "Sideway 1633", "age": 38, "salary": 60000}
    ]
    mycol.insert_many(dictionary)


def display_values(dbname,collection_name):
    '''
    Description:
        This fucntion is used to dispaly all the values in the collection.
    '''
    db = myclient[dbname]
    mycol = db[collection_name]
    for x in mycol.find():
        print(x)







if __name__ == '__main__':
    
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    show_databases()
    use_or_create_database('school')
    create_collection('school', 'student')
    insert_one('school','student')
    show_databases()
    show_collections('school')
    drop_database('school')
    print("\n ")
    #To create a new database employee and add details.
    insert_one('employee','employee_details')
    display_values('employee', 'employee_details')
    show_collections('employee')
    show_databases()
    insert_many('employee', 'employee_details')
    display_values('employee', 'employee_details')
    
    
    drop_database('employee')


