`experiment-utils` is a *collection* of libraries for running Python experiments. Each of these libraries are designed to be minimal and have no dependence between each library.

# List of Libraries

`sweep_configs`: A small library for running many independent experiments to sweep over different configurations. Supports parallelization of runs using mpi.

`data_io`: Handles saving and loading of data into a file-based ZODB system. The library assumes that each data point is associated with string id and config of some form. Additional support exists for JSON-able configs.

`analysis_common`: A collection of utilities around analyzing data such as smoothing. 