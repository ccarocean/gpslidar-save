Program for saving lidar and GPS data from postgresql database
==============================================================

Title: gpslidar-save

Options
-------
optional arguments:
  -h, --help            show this help message and exit
  -d DIRECTORY, --directory DIRECTORY
                        Directory for output data. Default is /srv/data/gpslidar.

Installation
------------
Create virtual environment:

.. code-block::

    python -m venv --prompt=save .venv
    source .venv/bin/activate

Inside Virtual Environment:

.. code-block::

    python setup.py install


How to run
----------
Source Virtual Environment:

.. code-block::

    source .venv/bin/activate

Run:

.. code-block::

    gpslidar-save


Related Files
-------------
- LiDAR data directory must exist at /srv/data/gpslidar/lidar
- Raw GPS data directory must exist at /srv/data/gpslidar/rawgps
- GPS position data directory must exist at /srv/data/gpslidar/position

Author
------
Adam Dodge

University of Colorado Boulder

Colorado Center for Astrodynamics Research

Jet Propulsion Laboratory

Purpose
-------
This program is for taking data from the database containing gps and lidar data and saving it to a file for later use.
The program then deletes the data from the database to ensure a quick and clean database. It saves the raw gps data to
Rinex observation files to be used for high precision gps positioning.