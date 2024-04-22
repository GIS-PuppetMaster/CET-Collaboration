package org.apache.iotdb.db.queryengine.plan.execution;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeManager;
import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeService;
import org.apache.iotdb.db.queryengine.execution.exchange.source.ISourceHandle;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumn;
import org.checkerframework.checker.units.qual.C;

import java.util.ArrayList;
import java.util.List;

public class ReceiveTsBlock {
    private static final MPPDataExchangeManager MPP_DATA_EXCHANGE_MANAGER =
            MPPDataExchangeService.getInstance().getMPPDataExchangeManager();
    private static int fragmentid=0;
    public TsBlock receive(){
        final String queryId = "test_query";
        final TEndPoint remoteEndpoint = new TEndPoint("localhost", 10740);
        final TFragmentInstanceId remoteFragmentInstanceId = new TFragmentInstanceId(queryId, fragmentid++, "0");
        final String localPlanNodeId = "receive_test";
        final TFragmentInstanceId localFragmentInstanceId = new TFragmentInstanceId(queryId, fragmentid++, "0");
        long query_num=1;
        FragmentInstanceContext instanceContext = new FragmentInstanceContext(query_num);
        ISourceHandle sourceHandle =MPP_DATA_EXCHANGE_MANAGER.createSourceHandle(
                localFragmentInstanceId,
                localPlanNodeId,
                0,//IndexOfUpstreamSinkHandle
                remoteEndpoint,
                remoteFragmentInstanceId,
                instanceContext::failed);
        ListenableFuture<?> isBlocked = sourceHandle.isBlocked();
        while (!isBlocked.isDone()) {
            try {
//                System.out.println("start to sleep 10");
                Thread.sleep(10);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        TsBlock tsBlock_rev = null;
        if (!sourceHandle.isFinished()) {
            tsBlock_rev = sourceHandle.receive();
            sourceHandle.close();
            Column[] valueColumns = tsBlock_rev.getValueColumns();
            System.out.println("receive columns:");
            for(Column valueColumn:valueColumns){
                System.out.println(valueColumn);
            }
            TimeColumn timeColumn_rev=tsBlock_rev.getTimeColumn();
            long[] times=timeColumn_rev.getTimes();
            System.out.println("receive time columns:");
            for(long time:times){
                System.out.println(time);
            }

        }
        return tsBlock_rev;
    }
}
