from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

class Cassandradb:
    def __init__(self,clusteraddr,username:str,password:str,keyspace:str):
        
        self.auth_provider = PlainTextAuthProvider(username=username, password=password)
        self.cluster = Cluster([clusteraddr],auth_provider=self.auth_provider)
        self.session = self.cluster.connect(keyspace=keyspace)
        
    
    def insert(self,table:str,columns:str,values:str):
        """columns ex. id,name Values ex. 2,Alex"""
        self.session.execute(f"INSERT INTO {table}({columns}) VALUES ({values})")
    
    
    def remove(self,table:str,pk:str):
        """ex. remove(tbl1,id=4)"""
        self.session.execute(f"delete from {table} where {pk}")
    
    def GetinList(self,query:str)->list:
        """query example:: SELECT name, age, email FROM users"""
        output=[]
        records = self.session.execute(query)
        
        for rec in records:
            output.append(rec)
        return output
    def GetbyMultiArgs(self,tablename:str,args:list,pkcolumn:str)->list:
        """This function can execute multiple select queries then returns a list"""

        user_lookup_stmt = self.session.prepare(f"SELECT * FROM {tablename} WHERE {pkcolumn}=?")

        result = []
        for arg in args:
            val = self.session.execute(user_lookup_stmt, [arg])
            result.append(val[0])
        return result
