import ot_db_manager


db_man = ot_db_manager.ot_db_manager('10.0.10.96', 'writer', 'LA-fcYg6SlqH', 'staging', 'OTLOG')

# DROP USER 'writer'@'%';
# DROP USER 'writer'@'10.0.10.223';
# CREATE USER 'writer'@'%' IDENTIFIED WITH 'mysql_native_password' BY 'LA-fcYg6SlqH';
# GRANT ALL PRIVILEGES ON staging.* TO 'writer'@'%';
# FLUSH PRIVILEGES;
