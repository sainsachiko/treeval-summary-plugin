package nextflow.treevalsummary

import groovy.transform.CompileStatic
import nextflow.Session
import nextflow.trace.TraceObserver
import nextflow.trace.TraceObserverFactory

@CompileStatic
class TreevalSummaryFactory implements TraceObserverFactory {

    @Override
    Collection<TraceObserver> create(Session session) {
        def file = session.getBaseDir().resolve(TreevalSummaryTraceData.DEF_FILE_NAME)

        final result = new ArrayList<TraceObserver>()
        result.add( new TreevalSummaryObserver() )
        result.add( new TreevalSummaryTraceData(file) )
        return result
    }
}