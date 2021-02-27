__author__ = "Ezequiel Alarcon"
__email__ = "zekalarcon@gmail.com"


import csv
import json
import requests
import sqlite3
import sqlalchemy
from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship


# Creo el motor (engine) de la base de datos
engine = sqlalchemy.create_engine("sqlite:///articulos.db")
base = declarative_base()
session = sessionmaker(bind=engine)()

# Creo tabla

class Articulo(base):
    __tablename__ = 'articulo'
    id = Column(String, primary_key=True, autoincrement=False)
    site_id = Column(String)
    title = Column(String)
    price = Column(Integer)
    currency_id = Column(String)
    initial_quantity = Column(Integer)
    available_quantity = Column(Integer)
    sold_quantity = Column(Integer)

    def __repr__(self):
        return f'Articulo\nId: {self.id}\nSite id: {self.site_id}\nTitle: {self.title}\
                 \nPrice: {self.price}\nCurrency id: {self.currency_id},\nInitial Quantity: {self.initial_quantity}\
                 \nAvailable Quantity: {self.initial_quantity}\nSold Quantity: {self.sold_quantity}'


def create_schema():

    base.metadata.drop_all(engine)

    base.metadata.create_all(engine)


def fill():

    contador = 0

    with open('technical_challenge_data.csv', 'r') as f:
        data = list(csv.DictReader(f))

        for x in data:
            item = x['site'] + x['id']

            url = 'https://api.mercadolibre.com/items?ids={}'.format(item)
            response = requests.get(url)
            data = response.json()

            filter_data = [data[0]['body']]
            
            for x in filter_data:
                try:
                  articulo = Articulo(id=x['id'], site_id=x['site_id'], title=x['title'], price=x['price'],
                                      currency_id=x['currency_id'], initial_quantity=x['initial_quantity'],
                                      available_quantity=x['available_quantity'], sold_quantity=x['sold_quantity'])
              
                  session.add(articulo)
                  session.commit()

                except:
                  contador += 1
                  continue  

    print(f'Articulos corruptos: {contador}')   


def fetch(id):

    Session = sessionmaker(bind=engine)
    session = Session() 

    query = session.query(Articulo).filter(Articulo.id == id) 
    articulo = query.first()
     
    if articulo is None:
        print(f'Articulo: {id} no existe')
    else:
        print(articulo) 



if __name__ == "__main__":
  # Creo DB
  #create_schema()

  # Completo la DB con el CSV
  #fill()

  # Leer filas por articulo id
  id = 'MLA845041373'
  fetch(id)
  

