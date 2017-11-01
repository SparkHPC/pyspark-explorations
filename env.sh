ANACONDA=$HOME/anaconda3
VENV=sparkdemo36
export PATH=$ANACONDA/envs/$VENV/bin/python:$PATH
export PATH=$ANACONDA/bin:$PATH
export PYSPARK_DRIVER_PYTHON=jupyter
export PYTHONPATH=$ANACONDA/envs/$VENV/bin/python
export PYSPARK_PYTHON=$ANACONDA/envs/$VENV/bin/python
export PYSPARK_DRIVER_PYTHON_OPTS="notebook --no-browser --ip=$(hostname).cooley.pub.alcf.anl.gov --port=8002" pyspark
export PYSPARK_MASTER_URI=spark://$(hostname):7077
