Program for saving lidar and GPS data from postgresql database
==============================================================

Title: gpslidar-save

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
- Desired output data directory, most likely /srv/data/gpslidar/ on cods
- GPSLIDARDATADIRECTORY environment variable is read to give location to save data
- GPSLIDAR_DNAME is read to give database URI (with credentials, if necessary)

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
Rinex observation files to be used for high precision gps positioning. It also averages the lidar data down to six
minute data for comparison with NOAA data.