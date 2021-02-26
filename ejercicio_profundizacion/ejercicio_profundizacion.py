__author__ = "Ezequiel Alarcon"
__email__ = "zekalarcon@gmail.com"


import os
import csv
import sqlite3
import sqlalchemy
from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship

# Creo el motor (engine) de la base de datos
engine = sqlalchemy.create_engine("sqlite:///libros.db")
base = declarative_base()
session = sessionmaker(bind=engine)()

from config1 import config

# Obtener la path de ejecución actual del script
script_path = os.path.dirname(os.path.realpath(__file__))

# Obtener los parámetros del archivo de configuración
config_path_name = os.path.join(script_path, 'config1.ini')
dataset = config('dataset', config_path_name)

# Creo tablas
class Autor(base):
    __tablename__ = "autor"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    
    def __repr__(self):
        return f"Autor: {self.name}"


class Libro(base):
    __tablename__ = "libro"
    id = Column(Integer, primary_key=True)
    title = Column(String)
    pags = Column(Integer)
    id_autor = Column(Integer, ForeignKey("autor.id"))

    autor = relationship("Autor")

    def __repr__(self):
        return f"Libro:{self.title}, Cantidad de pags: {self.pags}, Autor: {self.autor.name}"


def create_schema():

    base.metadata.drop_all(engine)

    base.metadata.create_all(engine)   


def fill():

    with open(dataset['autor']) as f:
        data = list(csv.DictReader(f))
        
        for row in data:
            autor_name = Autor(name=row['autor'])

            session.add(autor_name)
            session.commit() 


    with open(dataset['libro']) as f:
        data = list(csv.DictReader(f))

        for row in data:
            query = session.query(Autor.id).filter(Autor.name == row['autor'])
            autor_id = query.first()
            libro = Libro(title=row['titulo'], pags=row['cantidad_paginas'], id_autor=autor_id[0])        

            session.add(libro)
            session.commit() 


def fetch(id):
    
    Session = sessionmaker(bind=engine)
    session = Session()

    if id == 0:    
        query = session.query(Libro)
        libros = query.all()
        for libro in libros:
            print(libro)
    else:
        query = session.query(Libro).filter(Libro.id == id)
        libro = query.first()
        print(libro)


def search_author(book_title):

    Session = sessionmaker(bind=engine)
    session = Session()

    query = session.query(Autor).join(Libro).filter(Libro.title == book_title)
    libro = query.first()
    return libro

    

if __name__ == "__main__":
  # Creo DB
  create_schema()

  # Completo la DB con el CSV
  fill()

  # Leer filas
  # 0 para ver todo el contenido de la DB
  # Mayor a 0 para buscar por id especifico
  id = 2
  fetch(id)  
 

  # Buscar autor por titulo del libro
  book_title = 'El amor en los tiempos del colera'
  print(search_author(book_title))