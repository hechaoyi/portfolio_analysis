from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate


db = SQLAlchemy()

def create_app():
	app = Flask(__name__)
	app.config.from_object(Config)
	db.init_app(app)
	Migrate(app, db)
	app.shell_context_processor(lambda: {'db': db})
	return app

class Config:
	DEBUG = True
	SQLALCHEMY_DATABASE_URI = 'sqlite:///db'
	SQLALCHEMY_TRACK_MODIFICATIONS = False
