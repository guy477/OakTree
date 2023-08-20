import ot_db_manager


db_man = ot_db_manager.ot_db_manager('100.98.39.58', 'writer', 'LA-fcYg6SlqH', 'STAGING', 'OTLOG')

# DROP USER 'writer'@'%';
# DROP USER 'writer'@'10.0.10.223';
# CREATE USER 'writer'@'%' IDENTIFIED WITH 'mysql_native_password' BY 'LA-fcYg6SlqH';
# GRANT ALL PRIVILEGES ON STAGING.* TO 'writer'@'%';
# FLUSH PRIVILEGES;
