package zyh.service;

import org.apache.iotdb.db.queryengine.plan.execution.ReadFromCloudFlag;
import org.apache.iotdb.db.zcy.service.CtoEService;
import org.apache.iotdb.db.zcy.service.TSInfo;

import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.TException;
import org.apache.thrift.transport.layered.TFramedTransport;

public class SendData{
    public static int fragmentid;
    public SendData(){
        fragmentid=0;
    }
    public void send(){
        //多线程非阻塞版本
        TTransport transport = null;
        try  {
        transport =  new TFramedTransport(new TSocket("localhost", 9090));
        TProtocol protocol = new TBinaryProtocol(transport);
        CtoEService.Client client = new CtoEService.Client(protocol);
        transport.open();
        // 调用服务方法
        TSInfo dataToSend = new TSInfo(11, fragmentid, 13, 14);
        ReadFromCloudFlag readFromCloudFlag=ReadFromCloudFlag.getInstance();
        readFromCloudFlag.setFlag(true);
        client.sendData(dataToSend);
        System.out.println("Data sent successfully.");
        fragmentid+=2;


        } catch (TException x) {
            x.printStackTrace();
        }finally {

            if(null!=transport){
                transport.close();
            }
        }
    }
    public int getFragmentid(){
        return fragmentid;
    }

}
