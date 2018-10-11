import flask
from webApp import app

@app.route('/')
def index():
    return flask.render_template('index.html')

@app.route('/company')
def company():
    return flask.render_template('company.html')

@app.route('/service')
def service():
    return flask.render_template('service.html')

@app.route('/product')
def product():
    return flask.render_template('product.html')
    
@app.route('/contact')
def contact():
    return flask.render_template('contact.html')

@app.route('/finish')
def finish():
    return flask.render_template('finish.html')
    

    
