-- Eliminar esquema netflix si ya existe
DROP SCHEMA IF EXISTS netflix CASCADE;

-- Crear esquema netflix
CREATE SCHEMA netflix;

-- Establecer el esquema como predeterminado para esta sesión
SET search_path TO netflix, public;

-- Crear tabla Type_Title
CREATE TABLE netflix.Type_Title (
    id_type INT NOT NULL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    state BOOLEAN NOT NULL DEFAULT TRUE 
);

-- Crear tabla Director
CREATE TABLE netflix.Director (
    id_director INT NOT NULL PRIMARY KEY,
    name VARCHAR(150) NOT NULL,
    state BOOLEAN NOT NULL DEFAULT TRUE
);

-- Crear tabla Rating
CREATE TABLE netflix.Rating (
    id_rating INT NOT NULL PRIMARY KEY,
    rating_name VARCHAR(20) NOT NULL,
    state BOOLEAN NOT NULL DEFAULT TRUE 
);

-- Crear tabla Duration
CREATE TABLE netflix.Duration (
    id_duration INT NOT NULL PRIMARY KEY,
    duration_value INT NOT NULL,
    duration_unit VARCHAR(20) NOT NULL,
    state BOOLEAN NOT NULL DEFAULT TRUE
);

-- Crear tabla Title
CREATE TABLE netflix.Title (
    show_id VARCHAR(20) PRIMARY KEY,
    id_type INT NOT NULL,
    title VARCHAR(300) NOT NULL,
    id_director INT,
    date_added DATE,
    release_year INT,
    id_rating INT,
    id_duration INT,
    description TEXT,
    state BOOLEAN NOT NULL DEFAULT TRUE,
    CONSTRAINT fk_title_type FOREIGN KEY (id_type) REFERENCES netflix.Type_Title(id_type),
    CONSTRAINT fk_title_director FOREIGN KEY (id_director) REFERENCES netflix.Director(id_director),
    CONSTRAINT fk_title_rating FOREIGN KEY (id_rating) REFERENCES netflix.Rating(id_rating),
    CONSTRAINT fk_title_duration FOREIGN KEY (id_duration) REFERENCES netflix.Duration(id_duration)
);

-- Crear tabla Actor
CREATE TABLE netflix.Actor (
    id_actor INT NOT NULL PRIMARY KEY,
    name VARCHAR(150) NOT NULL,
    state BOOLEAN NOT NULL DEFAULT TRUE
);

-- Crear tabla Actor_Title
CREATE TABLE netflix.Actor_Title (
    show_id VARCHAR(20) NOT NULL,
    id_actor INT NOT NULL,
    state BOOLEAN NOT NULL DEFAULT TRUE,
    PRIMARY KEY (show_id, id_actor),
    CONSTRAINT fk_actor_title_show FOREIGN KEY (show_id) REFERENCES netflix.Title(show_id),
    CONSTRAINT fk_actor_title_actor FOREIGN KEY (id_actor) REFERENCES netflix.Actor(id_actor)
);

-- Crear tabla Country
CREATE TABLE netflix.Country (
    id_country INT NOT NULL PRIMARY KEY,
    country VARCHAR(100) NOT NULL,
    state BOOLEAN NOT NULL DEFAULT TRUE
);

-- Crear tabla Country_Title
CREATE TABLE netflix.Country_Title (
    id_country INT NOT NULL,
    show_id VARCHAR(20) NOT NULL,
    state BOOLEAN NOT NULL DEFAULT TRUE,
    PRIMARY KEY (id_country, show_id),
    CONSTRAINT fk_country_title_country FOREIGN KEY (id_country) REFERENCES netflix.Country(id_country),
    CONSTRAINT fk_country_title_title FOREIGN KEY (show_id) REFERENCES netflix.Title(show_id)
);

-- Crear tabla Listed_In
CREATE TABLE netflix.Listed_In (
    id_listed_in INT NOT NULL PRIMARY KEY,
    listed_in VARCHAR(100) NOT NULL,
    state BOOLEAN NOT NULL DEFAULT TRUE
);

-- Crear tabla Listed_In_Title
CREATE TABLE netflix.Listed_In_Title (
    id_listed_in INT NOT NULL,
    show_id VARCHAR(20) NOT NULL,
    state BOOLEAN NOT NULL DEFAULT TRUE,
    PRIMARY KEY (id_listed_in, show_id),
    CONSTRAINT fk_listedintitle_listedin FOREIGN KEY (id_listed_in) REFERENCES netflix.Listed_In(id_listed_in),
    CONSTRAINT fk_listedintitle_title FOREIGN KEY (show_id) REFERENCES netflix.Title(show_id)
);

-- Mensaje de confirmación
SELECT 'Esquema "netflix" y todas las tablas creadas exitosamente' AS mensaje;