package nextflow.treevalsummary

import groovy.transform.CompileStatic
import nextflow.plugin.BasePlugin
import nextflow.plugin.Scoped
import org.pf4j.PluginWrapper

/**
 * TreevalSummaryPlugin entrypoint
 */
 
@CompileStatic
class TreevalSummaryPlugin extends BasePlugin {

    TreevalSummaryPlugin(PluginWrapper wrapper) {
        super(wrapper)
    }
}
