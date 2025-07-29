import pandas as pd
from collections import defaultdict

class NetflixDataSeparator:
    def __init__(self, csv_file_path):
        self.csv_file_path = csv_file_path
        self.df = None
        
        # Diccionarios Para Almacenar Datos
        self.type_shows = {}
        self.directors = {}
        self.ratings = {}
        self.durations = {}
        self.actors = {}
        self.countries = {}
        self.listed_in = {}
        
        # Listas Para Tabla De Relaciones
        self.titles = []
        self.actor_title_relations = []
        self.country_title_relations = []
        self.listed_in_title_relations = []
        
        # Contadores Para IDs
        self.type_id_counter = 1
        self.director_id_counter = 1
        self.rating_id_counter = 1
        self.duration_id_counter = 1
        self.actor_id_counter = 1
        self.country_id_counter = 1
        self.listed_in_id_counter = 1

    def load_csv(self):
        print("Cargando Archivo CSV")
        
        self.df = pd.read_csv(
            self.csv_file_path,
            na_values=['', 'nan', 'NaN'],
            keep_default_na=False,     
            dtype=str                
        )
        
        expected_columns = [
            'show_id', 'type', 'title', 'director', 'cast', 
            'country', 'date_added', 'release_year', 'rating', 
            'duration', 'listed_in', 'description'
        ]
        
        print(f"Cargados {len(self.df)} registros")
        
        if list(self.df.columns) != expected_columns:
            print("El orden de columnas no coincide con lo esperado.")
            print(f"Esperado: {expected_columns}")
            print(f"Encontrado: {list(self.df.columns)}")
        
        self.df = self.df.fillna('')
        
        for col in self.df.columns:
            if self.df[col].dtype == 'object':
                self.df[col] = self.df[col].str.strip()

    def extract_type_shows(self):
        print("Extrayendo Tipos De Titles")
        
        for type_name in self.df['type'].unique():
            if type_name and type_name not in self.type_shows:
                self.type_shows[type_name] = self.type_id_counter
                self.type_id_counter += 1

    def extract_directors(self):
        print("Extrayendo Directores")
        
        # Agregar Un Director Por Defecto
        self.directors['Unknown Director'] = self.director_id_counter
        self.director_id_counter += 1
        
        for directors_str in self.df['director']:
            if directors_str and directors_str.strip():
                # Dividir múltiples directores por coma
                directors_list = [d.strip() for d in directors_str.split(',')]
                for director in directors_list:
                    if director and director not in self.directors:
                        self.directors[director] = self.director_id_counter
                        self.director_id_counter += 1

    def extract_ratings(self):
        print("Extrayendo Ratings")
        
        # Agregar Un Rating Por Defecto
        self.ratings['Not Rated'] = self.rating_id_counter
        self.rating_id_counter += 1
        
        for rating in self.df['rating'].unique():
            if rating and rating.strip() and rating not in self.ratings:
                self.ratings[rating] = self.rating_id_counter
                self.rating_id_counter += 1

    def extract_durations(self):
        print("Extrayendo Duraciones")
        
        # Agregar Una Duración Por Defecto
        self.durations['0|No Duration'] = {
            'id': self.duration_id_counter,
            'value': 0,
            'unit': 'No Duration'
        }
        self.duration_id_counter += 1
        
        for duration_str in self.df['duration']:
            if duration_str and duration_str.strip():
                # Separa duración en valor y unidad
                parts = duration_str.strip().split(None, 1)
                if len(parts) >= 2:
                    try:
                        duration_value = int(parts[0])
                        duration_unit = parts[1] if len(parts) > 1 else 'Unknown'
                        duration_key = f"{duration_value}|{duration_unit}"
                        if duration_key not in self.durations:
                            self.durations[duration_key] = {
                                'id': self.duration_id_counter,
                                'value': duration_value,
                                'unit': duration_unit
                            }
                            self.duration_id_counter += 1
                    except ValueError:
                        continue

    def extract_actors(self):
        print("Extrayendo Actores")
        
        # Agregar Un Actor Por Defecto
        self.actors['Unknown Actor'] = self.actor_id_counter
        self.actor_id_counter += 1
        
        for cast_str in self.df['cast']:
            if cast_str and cast_str.strip():
                # Dividir múltiples actores por coma
                actors_list = [actor.strip() for actor in cast_str.split(',')]
                for actor in actors_list:
                    if actor and actor not in self.actors:
                        self.actors[actor] = self.actor_id_counter
                        self.actor_id_counter += 1

    def extract_countries(self):
        print("Extrayendo Países")
        
        # Agregar Un País Por Defecto
        self.countries['Unknown Country'] = self.country_id_counter
        self.country_id_counter += 1
        
        for country_str in self.df['country']:
            if country_str and country_str.strip():
                # Dividir múltiples países por coma
                countries_list = [country.strip() for country in country_str.split(',')]
                for country in countries_list:
                    if country and country not in self.countries:
                        self.countries[country] = self.country_id_counter
                        self.country_id_counter += 1

    def extract_listed_in(self):
        print("Extrayendo Categorías")
        
        # Agregar Una Categoría Por Defecto
        self.listed_in['No Listed In'] = self.listed_in_id_counter
        self.listed_in_id_counter += 1
        
        for listed_str in self.df['listed_in']:
            if listed_str and listed_str.strip():
                # Dividir múltiples géneros por coma
                genres_list = [genre.strip() for genre in listed_str.split(',')]
                for genre in genres_list:
                    if genre and genre not in self.listed_in:
                        self.listed_in[genre] = self.listed_in_id_counter
                        self.listed_in_id_counter += 1

    def create_titles_and_relations(self):
        print("Creando Titles y Relaciones.")
        
        for index, row in self.df.iterrows():
            show_id = row['show_id']
            
            # Datos para la tabla Title usando valores por defecto si están vacíos
            title_data = {
                'show_id': show_id,
                'id_type': self._get_type_id(row['type']),
                'title': row['title'] if row['title'] else 'Unknown Title',
                'id_director': self._get_director_id(row['director']),
                'date_added': self._parse_date(row['date_added']),
                'release_year': self._parse_year(row['release_year']),
                'id_rating': self._get_rating_id(row['rating']),
                'id_duration': self._get_duration_id(row['duration']),
                'description': row['description'] if row['description'] else ''
            }
            self.titles.append(title_data)
            
            # Relaciones Actor-Title
            self._create_actor_relations(show_id, row['cast'])
            
            # Relaciones Country-Title
            self._create_country_relations(show_id, row['country'])
            
            # Relaciones Listed_In-Title
            self._create_listed_in_relations(show_id, row['listed_in'])

    def _get_type_id(self, type_str):
        """Obtener ID del tipo"""
        if not type_str or not type_str.strip():
            return 1  # ID por defecto
        return self.type_shows.get(type_str, 1)

    def _get_director_id(self, director_str):
        """Obtener ID del director, usar Unknown si está vacío"""
        if not director_str or not director_str.strip():
            return self.directors['Unknown Director']
        
        # Si hay múltiples directores, tomar el primero
        first_director = director_str.split(',')[0].strip()
        return self.directors.get(first_director, self.directors['Unknown Director'])

    def _get_rating_id(self, rating_str):
        """Obtener ID del rating"""
        if not rating_str or not rating_str.strip():
            return self.ratings['Not Rated']
        return self.ratings.get(rating_str, self.ratings['Not Rated'])

    def _get_duration_id(self, duration_str):
        """Obtener ID de la duración"""
        if not duration_str or not duration_str.strip():
            return self.durations['0|No Duration']['id']
        
        parts = duration_str.strip().split(None, 1)
        if len(parts) >= 2:
            try:
                duration_value = int(parts[0])
                duration_unit = parts[1]
                duration_key = f"{duration_value}|{duration_unit}"
                return self.durations.get(duration_key, {}).get('id', self.durations['0|No Duration']['id'])
            except ValueError:
                return self.durations['0|No Duration']['id']
        return self.durations['0|No Duration']['id']

    def _parse_date(self, date_str):
        if not date_str or not date_str.strip():
            return None
        
        clean_date = date_str.strip().replace('"', '')
        
        try:
            from datetime import datetime
            parsed_date = datetime.strptime(clean_date, "%B %d, %Y")
            return parsed_date.strftime("%Y-%m-%d")
        except ValueError:
            try:
                parsed_date = datetime.strptime(clean_date, "%m/%d/%Y")
                return parsed_date.strftime("%Y-%m-%d")
            except ValueError:
                return None

    def _parse_year(self, year_str):
        if not year_str or not year_str.strip():
            return None
        try:
            return int(year_str)
        except (ValueError, TypeError):
            return None

    def _create_actor_relations(self, show_id, cast_str):
        """Crear relaciones actor-título"""
        if not cast_str or not cast_str.strip():
            # Usar Unknown Actor por defecto si no hay cast
            self.actor_title_relations.append({
                'show_id': show_id,
                'id_actor': self.actors['Unknown Actor']
            })
        else:
            actors_list = [actor.strip() for actor in cast_str.split(',')]
            for actor in actors_list:
                if actor and actor in self.actors:
                    self.actor_title_relations.append({
                        'show_id': show_id,
                        'id_actor': self.actors[actor]
                    })

    def _create_country_relations(self, show_id, country_str):
        """Crear relaciones país-título"""
        if not country_str or not country_str.strip():
            # Usar Unknown Country por defecto
            self.country_title_relations.append({
                'id_country': self.countries['Unknown Country'],
                'show_id': show_id
            })
        else:
            countries_list = [country.strip() for country in country_str.split(',')]
            for country in countries_list:
                if country and country in self.countries:
                    self.country_title_relations.append({
                        'id_country': self.countries[country],
                        'show_id': show_id
                    })

    def _create_listed_in_relations(self, show_id, listed_str):
        """Crear relaciones género-título"""
        if not listed_str or not listed_str.strip():
            # Usar No Listed In por defecto
            self.listed_in_title_relations.append({
                'id_listed_in': self.listed_in['No Listed In'],
                'show_id': show_id
            })
        else:
            genres_list = [genre.strip() for genre in listed_str.split(',')]
            for genre in genres_list:
                if genre and genre in self.listed_in:
                    self.listed_in_title_relations.append({
                        'id_listed_in': self.listed_in[genre],
                        'show_id': show_id
                    })

    def process_all(self):
        """Procesar todos los datos"""
        self.load_csv()
        self.extract_type_shows()
        self.extract_directors()
        self.extract_ratings()
        self.extract_durations()
        self.extract_actors()
        self.extract_countries()
        self.extract_listed_in()
        self.create_titles_and_relations()

    def save_to_csv(self, output_folder='output'):
        import os
        # Crear carpeta de salida
        os.makedirs(output_folder, exist_ok=True)
        
        print(f"Guardando Datos En La Carpeta: '{output_folder}'.")
        
        # Guardar Type_Title
        type_df = pd.DataFrame([
            {'id_type': id_val, 'name': name} 
            for name, id_val in self.type_shows.items()
        ])
        type_df.to_csv(f'{output_folder}/type_title.csv', index=False)
        
        # Guardar Directors
        director_df = pd.DataFrame([
            {'id_director': id_val, 'name': name} 
            for name, id_val in self.directors.items()
        ])
        director_df.to_csv(f'{output_folder}/director.csv', index=False)
        
        # Guardar Ratings
        rating_df = pd.DataFrame([
            {'id_rating': id_val, 'rating_name': name} 
            for name, id_val in self.ratings.items()
        ])
        rating_df.to_csv(f'{output_folder}/rating.csv', index=False)
        
        # Guardar Durations
        duration_df = pd.DataFrame([
            {'id_duration': data['id'], 'duration_value': data['value'], 'duration_unit': data['unit']} 
            for data in self.durations.values()
        ])
        duration_df.to_csv(f'{output_folder}/duration.csv', index=False)
        
        # Guardar Actors
        actor_df = pd.DataFrame([
            {'id_actor': id_val, 'name': name} 
            for name, id_val in self.actors.items()
        ])
        actor_df.to_csv(f'{output_folder}/actor.csv', index=False)
        
        # Guardar Countries
        country_df = pd.DataFrame([
            {'id_country': id_val, 'country': name} 
            for name, id_val in self.countries.items()
        ])
        country_df.to_csv(f'{output_folder}/country.csv', index=False)
        
        # Guardar Listed_In
        listed_df = pd.DataFrame([
            {'id_listed_in': id_val, 'listed_in': name} 
            for name, id_val in self.listed_in.items()
        ])
        listed_df.to_csv(f'{output_folder}/listed_in.csv', index=False)
        
        # Guardar Titles
        titles_df = pd.DataFrame(self.titles)
        titles_df.to_csv(f'{output_folder}/title.csv', index=False)
        
        # Guardar relaciones
        pd.DataFrame(self.actor_title_relations).to_csv(f'{output_folder}/actor_title.csv', index=False)
        pd.DataFrame(self.country_title_relations).to_csv(f'{output_folder}/country_title.csv', index=False)
        pd.DataFrame(self.listed_in_title_relations).to_csv(f'{output_folder}/listed_in_title.csv', index=False)
        
        print("Archivos Guardados Exitosamente.")

    def print_summary(self):
        print("\n" + "="*50)
        print("Resumen de Datos")
        print("="*50)
        print(f"Tipos de show: {len(self.type_shows)}")
        print(f"Directores: {len(self.directors)}")
        print(f"Ratings: {len(self.ratings)}")
        print(f"Duraciones: {len(self.durations)}")
        print(f"Actores: {len(self.actors)}")
        print(f"Países: {len(self.countries)}")
        print(f"Categorías: {len(self.listed_in)}")
        print(f"Títulos: {len(self.titles)}")
        print(f"Relaciones Actor-Título: {len(self.actor_title_relations)}")
        print(f"Relaciones País-Título: {len(self.country_title_relations)}")
        print(f"Relaciones Género-Título: {len(self.listed_in_title_relations)}")

# Uso del código
if __name__ == "__main__":
    separator = NetflixDataSeparator('netflix_titles.csv')
    
    # Procesar todos los datos
    separator.process_all()
    
    # Mostrar resumen
    separator.print_summary()
    
    # Guardar en archivos CSV separados
    separator.save_to_csv('netflix_separated_data')
    
    print("\nProceso completado exitosamente.")