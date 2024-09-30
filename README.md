This is a demo project. 
It demonstrates the use of replacement for the Extraction layer with a test fixture. 

# demo-datafixture

You will need Poetry tool for dependency management. Install it using the [Poetry Installation Guide](https://python-poetry.org/docs/#installation) .

First, use ``poetry env use `which python` `` to set up Python enterperter you are using.

Second, use ``poetry install`` to collect dependencies for the environment.

Thrid, use ``poetry shell`` to enter the environment.  


Now you may run data_gen: ``python data_gen.py`` to create the data fixtures directory with input data and expectations files.

Use ``python create_db.py`` to create a test database.


Demo pipeline ``python demo/pipeline.py``

Pytest ``pytest``.
