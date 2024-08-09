CREATE DATABASE google_yelp;
USE google_yelp;
SET SQL_SAFE_UPDATES = 0;
SET GLOBAL wait_timeout = 28800;
SET GLOBAL interactive_timeout = 28800;

-- creacion tabla estado
DROP TABLE IF EXISTS Estado;
CREATE TABLE IF NOT EXISTS Estado(
Id_Estado INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
Estado VARCHAR(50)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- creacion de tabla ciudad 
DROP TABLE IF EXISTS Ciudad;
CREATE TABLE IF NOT EXISTS Ciudad(
Id_Ciudad INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
Ciudad VARCHAR (100),
Estado VARCHAR (50),
Id_Estado INT) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- tabla categorias
DROP TABLE IF EXISTS Categoria;
CREATE TABLE IF NOT EXISTS Categoria(
Id_Categoria INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
Categoria VARCHAR (50)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- tabla negocios yelp
DROP TABLE IF EXISTS Negocios_Yelp;
CREATE TABLE Negocios_Yelp(
Id_Negocio VARCHAR(200) PRIMARY KEY,
Nombre VARCHAR(200),
Id_Ciudad INT,
Ciudad VARCHAR(100),
Direccion VARCHAR(200),
Latitud FLOAT,
Longitud FLOAT,
Estrellas FLOAT,
Id_Categoria INT,
Num_Reviews INT,
Categoria VARCHAR(50)
)ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- tabla negocios google
DROP TABLE IF EXISTS Negocios_Google;
CREATE TABLE Negocios_Google(
Id_Negocio VARCHAR(200) UNIQUE,
Nombre VARCHAR(1000),
Direccion VARCHAR(300),
Latitud FLOAT,
Longitud FLOAT,
Calificacion FLOAT,
Id_Categoria INT,
Categoria Varchar(50),
Num_Reviews INT,
URL VARCHAR(200),
Result_Rel VARCHAR(2000) 
)ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- tabla reviews yelp
DROP TABLE IF EXISTS Reviews_Yelp;
CREATE TABLE Reviews_Yelp(
Id_Review INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
Id_Negocio VARCHAR(200),
Estrellas FLOAT,
Reseña VARCHAR(6000),
Fecha DATETIME 
)ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;

-- tabla auxiliar
DROP TABLE aux;
CREATE TABLE aux(
str VARCHAR (160),
str2 VARCHAR(160),
inter INT,
inter2 INT)
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- carga de tablas

-- categorias
LOAD DATA INFILE "C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\yelp_final.csv"
INTO TABLE aux
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(@dummy,@dummy,@dummy,@dummy,@dummy,@dummy,@dummy,@dummy,@dummy,@dummy,str);

Select * FROM aux;

INSERT INTO categoria(Categoria)
SELECT DISTINCT str FROM aux;

Select * FROM Categoria;

-- negocios yelp
LOAD DATA INFILE "C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\yelp_final.csv"
INTO TABLE negocios_yelp
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(Id_Negocio,Nombre,Direccion, Ciudad, @dummy, @dummy, Latitud,Longitud, Estrellas, Num_Reviews,Categoria);

SELECT * FROM negocios_yelp;

UPDATE negocios_yelp n
JOIN categoria c
USING (Categoria)
SET n.Id_Categoria=c.Id_Categoria;

ALTER TABLE negocios_yelp DROP Categoria;


-- negocios google
LOAD DATA INFILE "C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\google_final.csv"
INTO TABLE negocios_google
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(Nombre,Direccion, Id_Negocio, Latitud,Longitud, Calificacion, Num_Reviews, Result_Rel, URL, Categoria);

SELECT * FROM negocios_google;

UPDATE negocios_google n
JOIN categoria c
USING (Categoria)
SET n.Id_Categoria=c.Id_Categoria;

ALTER TABLE negocios_google DROP Categoria;

-- estados
TRUNCATE TABLE aux;
LOAD DATA INFILE "C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\google_final.csv"
INTO TABLE aux
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(@dummy,@dummy,@dummy,@dummy,@dummy,@dummy,@dummy,@dummy,@dummy,@dummy,str);

INSERT INTO estado(Estado)
SELECT DISTINCT str FROM aux;
 
-- agregamos estados 
ALTER TABLE negocios_google
ADD column Estado VARCHAR (50),
ADD COLUMN Id_Estado INT;

TRUNCATE TABLE aux;
LOAD DATA INFILE "C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\google_final.csv"
INTO TABLE aux
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(@dummy,@dummy,str,@dummy,@dummy,@dummy,@dummy,@dummy,@dummy,@dummy,str2);

UPDATE negocios_google n
JOIN aux a
ON n.Id_Negocio=a.str
SET n.Estado=a.str2;

UPDATE negocios_google n
JOIN estado e
ON n.Estado=e.Estado
SET n.Id_Estado=e.Id_Estado;

ALTER TABLE negocios_google
DROP Estado;

-- reviews
TRUNCATE reviews_yelp;
LOAD DATA INFILE "C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\reviews_final1.csv"
INTO TABLE reviews_yelp
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(Id_Negocio,Estrellas,Reseña,@dummy);

LOAD DATA INFILE "C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\reviews_final2.csv"
INTO TABLE reviews_yelp
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(Id_Negocio,Estrellas,Reseña,@dummy);

LOAD DATA INFILE "C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\reviews_final3.csv"
INTO TABLE reviews_yelp
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(Id_Negocio,Estrellas,Reseña,@dummy);

-- ciudades
truncate aux;
LOAD DATA INFILE "C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\yelp_final.csv"
INTO TABLE aux
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(@dummy,@dummy,@dummy, str, str2, @dummy,@dummy,@dummy,@dummy,@dummy,@dummy);

INSERT INTO Ciudad(Ciudad,Estado)
SELECT DISTINCT str,str2 FROM aux;

UPDATE negocios_yelp y
JOIN ciudad c
ON c.Ciudad=y.Ciudad
SET y.Id_Ciudad = c.Id_Ciudad;

ALTER TABLE negocios_yelp DROP Ciudad;


UPDATE ciudad c
JOIN estado e
USING (Estado)
SET c.Id_Estado = e.Id_Estado;

SELECT c.Id_estado, c.Estado, e.Id_estado
FROM ciudad c
JOIN estado e ON c.Estado = e.Estado
WHERE c.Id_estado != e.Id_estado;

-- foreign keys
ALTER TABLE negocios_google 
ADD CONSTRAINT fk_Categoria
FOREIGN KEY (Id_Categoria) REFERENCES categoria(Id_Categoria);

ALTER TABLE negocios_google 
ADD CONSTRAINT fk_Estado
FOREIGN KEY (Id_Estado) REFERENCES Estado(Id_Estado);

ALTER TABLE negocios_yelp
ADD CONSTRAINT fk_Categoria_yelp
FOREIGN KEY (Id_Categoria) REFERENCES categoria(Id_Categoria);

ALTER TABLE negocios_yelp
ADD CONSTRAINT fk_ciudad_yelp
FOREIGN KEY (Id_Ciudad) REFERENCES ciudad(Id_Ciudad);

ALTER TABLE ciudad
ADD CONSTRAINT fk_ciudad_estado
FOREIGN KEY (Id_Estado) REFERENCES estado(Id_Estado);

ALTER TABLE reviews_yelp
ADD CONSTRAINT fk_negocio
FOREIGN KEY (Id_Negocio) REFERENCES negocios_yelp(Id_Negocio);