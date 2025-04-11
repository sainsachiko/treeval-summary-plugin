# treeval-summary plugin

This project contains a simple Nextflow plugin called `treeval-summary` to summary metadata and trace of a nextflow run, then store it in a defined DuckDB with 02 tables (meta and trace).

## Plugin structure

- `settings.gradle`

  Gradle project settings.

- `plugins/treeval-summary`

  The plugin implementation base directory.

- `plugins/treeval-summary/build.gradle`

  Plugin Gradle build file. Project dependencies should be added here.

- `plugins/treeval-summary/src/resources/META-INF/MANIFEST.MF`

  Manifest file defining the plugin attributes e.g. name, version, etc. The attribute `Plugin-Class` declares the plugin main class. This class should extend the base class `nextflow.plugin.BasePlugin` e.g. `nextflow.summary.SummaryPlugin`.

- `plugins/treeval-summary/src/resources/META-INF/extensions.idx`

  This file declares one or more extension classes provided by the plugin. Each line should contain the fully qualified name of a Java class that implements the `org.pf4j.ExtensionPoint` interface (or a sub-interface).

- `plugins/treeval-summary/src/main`

  The plugin implementation sources.

## Plugin classes

- `SummaryExtension`: shows how to create custom function
- `SummaryPlugin`: the plugin entry point

## Install the plugin

```bash
make install
```

## Use plugin in a pipeline

nextflow.config

```
plugins {
  id 'treeval-summary@0.0.1'
}
```

workflow

Function `summary` gather run log data and store that in a DuckDB. This function could be used as:

```
include { summary } from 'plugin/treeval-summary'

summary( workflow, params, metrics, dbPath)
```

Where metrics is information related to input data and dbPath is path to existing/desired duckdb to store log data.
