# Database_Integrity
To check primary key and foreign key integrity of PostgreSQL database.     
It can automatically generate SQL queries base on the input database schema file.     
After running these queries, it will geneate the rsult table and save it to database.     

# Resources
readme: introduction of the code            
refint.py: my code to check Primary key and Foreign key integrity       
refint.sql: generated SQL result of refint.py     
testcases_refint_ta.sql: the example database for testing usage     
dbxyz_ta.txt: example schema file for testing     

# How to run
1) run testcases_refint_ta.sql in postgreSQL to generate a test databse    
2) run cmd: python refint.py "Database=dbxyz_ta.txt; err=0.01"     

