'use strict';

const express = require('express');
const bodyParser = require('body-parser');
const cors = require('cors');
const morgan = require('morgan');

let network = require('./fabric/network.js');

const app = express();
app.use(morgan('combined'));
app.use(bodyParser.json());
app.use(cors());


app.get('/queryAllMts', (req, res) => {
    network.queryAllCars()
        .then((response) => {
            let carsRecord = JSON.parse(response);
            res.send(carsRecord);
        });
});

app.get('/querySingleMt', (req, res) => {
    console.log(req.query.key);
    network.querySingleCar(req.query.key)
        .then((response) => {
            let carsRecord = JSON.parse(response);
            res.send(carsRecord);
        });
});

app.post('/createMt', (req, res) => {
    console.log(req.body);
    network.queryAllCars()
        .then((response) => {
            console.log(response);
            let carsRecord = JSON.parse(response);
            let numCars = carsRecord.length;
            let newKey = 'MT' + numCars;
            network.createCar(newKey, req.body.tempA, req.body.tempB, req.body.tempC, req.body.owner)
                .then((response) => {
                    res.send(response);
                });
        });
});

app.post('/changeCarOwner', (req, res) => {
    network.changeCarOwner(req.body.key, req.body.newOwner)
        .then((response) => {
            res.send(response);
        });
});

app.listen(process.env.PORT || 8081);