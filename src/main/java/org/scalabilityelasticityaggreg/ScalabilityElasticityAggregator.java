package org.scalabilityelasticityaggreg;

import com.sun.org.apache.xpath.internal.SourceTree;
import org.kairosdb.client.builder.DataPoint;

import javax.sound.midi.Soundbank;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Created by dmetallidis on 9/20/16.
 */
public class ScalabilityElasticityAggregator {



    public static void main(String[] args) throws Exception {

        // 1st input parameter: A scaling point of interest (that we had a IaaS scaling)
        // 2nd name of the service for the precision calculation
        // 3rd input parameter: Number of precise adaptation/scaling actions (for software components) from the start of reporting of the external service  till the time of passing the argument to the aggregator
        // 4th input parameter: Total times of adaptation/scaling (for software components) from the start of reporting of the external service till the time of passing the argument to the aggregator

        // 5th input parameter: A table of dates of Start of the request of the software component in order to start to scale, the type of scaling should also given and correspond to end date
        // 6th input parameter: A table of dates of End of the software component as it is up and actually running, the type of scaling should also given and correspond to start date

        // 7th input parameter: A table of dates of Start scaling out procedures
        // 8th input parameter: A table of dates of End scaling out procedures

        // 9th input parameter : A table of dates of Start scaling in procedures
        // 10th input parameter : A table of dates of End scaling in procedures

        //11th input parameter : average amount of underprovisioned resources during the underprovisiond period given by 7th and 8th parameters
        //12th input parameter : average amount of overprovisioned resources during the overprovisioned period given by 9th and 10th parameters

        //13th input parameter : Number of SLO violations that represent the number of start/end dates of the scaleRequestStart/scaleRequestEnd, if we have five such periods of times we will have the addition of these five values

        ScalabilityElasticityAggregator http = new ScalabilityElasticityAggregator();

        KairosDbClient client = new KairosDbClient("http://localhost:8088");
        client = client.initializeFullBuilder(client);

        KairosDbClient centralClient = new KairosDbClient("http://" +args[0]+ ":8088");
        centralClient = centralClient.initializeFullBuilder(centralClient);

        // this is the SCALING POINT that we will take as a argument in the running of the jar
        Date scalingPoint = new Date(System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(15)); // 1st
        //for each of the services in case of precision we give the parameter -precision and then the name of the service and the two numbers
        ArrayList<String> serviceOfAdaptation = new ArrayList<String>();
        serviceOfAdaptation.add("user");    // 2nd
        ArrayList<Double> preciseNumberOfAdaptationsScales = new ArrayList<Double>(); // 3rd
        preciseNumberOfAdaptationsScales.add(10.0);
        ArrayList<Double> totalTimesOfAdaptationScaling = new ArrayList<Double>();//4th
        totalTimesOfAdaptationScaling.add(15.0);//

        ArrayList<Date> scaleRequestStartPoint = new ArrayList<Date>();// 5th
        scaleRequestStartPoint.add( new Date(System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(30)));
        scaleRequestStartPoint.add(new Date(System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(25)));


        ArrayList<Date> scaleRequestEndPoint = new ArrayList<Date>();// 6th
        scaleRequestEndPoint.add(new Date(System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(28)));
        scaleRequestEndPoint.add(new Date(System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(27)));


        ArrayList<Date> scaleOutRequestStartPoint = new ArrayList<Date>(); // 7th
        ArrayList<Date> scaleOutRequestEndPoint = new ArrayList<Date>();
        scaleOutRequestStartPoint.add(new Date(System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(30)));
        scaleOutRequestEndPoint.add(new Date(System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(28)));


        ArrayList<Date> scaleInRequestStartPoint = new ArrayList<Date>(); // 8th
        ArrayList<Date> scaleInRequestEndPoint = new ArrayList<Date>();
        scaleInRequestStartPoint.add(new Date(System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(25)));
        scaleInRequestEndPoint.add(new Date(System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(27)));

        double avgAmountOutProvisionedResources = 5.0;
        double avgAmountInProvisionedResources = 2.0;
        double nsolv = 3;

        System.out.println("Start retrieving and pushing metrics for scalability/elasticity in local KairosDB");
        http.retrieveScalabilityAdaptabilityMetrics(client, scalingPoint, serviceOfAdaptation, preciseNumberOfAdaptationsScales, totalTimesOfAdaptationScaling);
        http.retrieveElasticityAdaptabilityMetrics(client, scaleRequestStartPoint, scaleRequestEndPoint,
                scaleOutRequestStartPoint, scaleOutRequestEndPoint,
                scaleInRequestStartPoint, scaleInRequestEndPoint,
                avgAmountOutProvisionedResources, avgAmountInProvisionedResources,
                nsolv);

        System.out.println("Start retrieving and pushing metrics for scalability/elasticity in central KairosDB");
        http.retrieveScalabilityAdaptabilityMetrics(centralClient, scalingPoint, serviceOfAdaptation, preciseNumberOfAdaptationsScales, totalTimesOfAdaptationScaling);
        http.retrieveElasticityAdaptabilityMetrics(centralClient, scaleRequestStartPoint, scaleRequestEndPoint,
                scaleOutRequestStartPoint, scaleOutRequestEndPoint,
                scaleInRequestStartPoint, scaleInRequestEndPoint,
                avgAmountOutProvisionedResources, avgAmountInProvisionedResources,
                nsolv);

    }

    // scaleOut and scaleIn points should have the same number between them and the addition of the number
    // should be equal with the scaleRequestStartPoint and scaleRequestEndPoint that have the same number between them
    private void retrieveElasticityAdaptabilityMetrics(KairosDbClient client, ArrayList<Date> scaleRequestStartPoint, ArrayList<Date> scaleRequestEndPoint,
                                           ArrayList<Date> scaleOutRequestStartPoint, ArrayList<Date> scaleOutRequestEndPoint,
                                           ArrayList<Date> scaleInRequestStartPoint, ArrayList<Date> scaleInRequestEndPoint,
                                           double avgAmountOutProvisionedResources, double  avgAmountInProvisionedResources,
                                           double nsolv) throws Exception {

        Double meanTimeTakenToReact;
        Double minutes;
        Double seconds;
        Long timestamp ;
        Long dtimestamp;
        Object value ;
        Double dvalue;
        Double sumOfReactionValues = 0.0;
        String prefixesOfMaxServices [] = {"userMaxThroughput", "permAdminMaxThroughput", "objectMaxThroughput", "ouUserMaxThroughput", "roleMaxThroughput", "userDetailMaxThroughput", "permMaxThroughput", "objectAdminMaxThroughput", "roleAdminMaxThroughput"
                , "ouPermMaxThroughput", "sdDynamicMaxThroughput", "sdStaticMaxThroughput"};

        for(int i =0; i<scaleRequestEndPoint.size(); i++){
            meanTimeTakenToReact = Double.valueOf(scaleRequestEndPoint.get(i).getTime() -  scaleRequestStartPoint.get(i).getTime());
            minutes = Double.valueOf(meanTimeTakenToReact / 1000) / 60;
            seconds = minutes * 60;
            System.out.println("Seconds to be put are  : " + seconds + " and mean time to react is :" + meanTimeTakenToReact);
            client.putAlreadyInstantiateMetric("ReactionTime", new Date().getTime(), seconds);
        }

        // a thread sleep in order to push the metrics to to the TSDB
        Thread.sleep(3000);
        // and now the computation of the meanTimeTakeToReact
        List<DataPoint> listDapoint = client.QueryDataPointsAbsolute("ReactionTime", new Date(0), null);
        System.out.println("DataPoints Size equals with : "  + listDapoint.size() + " name of metric equals with : ReactionTime");

        if(listDapoint.size() >0) {
            System.out.println("Size of it equals to : " + listDapoint.size());
            // here we take the values that we just get

            for (int j = 0; j < listDapoint.size(); j++) {
                timestamp = listDapoint.get(j).getTimestamp();
                dtimestamp = Long.parseLong(timestamp.toString());
                value = listDapoint.get(j).getValue();
                dvalue = Double.parseDouble(value.toString());
                System.out.println("dvalue equals with : " + dvalue);
                sumOfReactionValues =  sumOfReactionValues + dvalue;
            }
        }

        // calculation of the mean time to react
        System.out.println("SumOfReactionValue is : " + sumOfReactionValues);
        client.putAlreadyInstantiateMetric("MeanTimeTakeToReact",  new Date().getTime(), sumOfReactionValues/listDapoint.size());


        double maxBeforeScale;
        double maxAfterScale;
        long timestampBeforeScale =0; // to be used for calculation of the mtqqr metric
        long timestampAfterScale = 0; // to be used for calculation of the mtqqr metric
        double additionOfPerfFactors =0;
        double timeToQualityRepair = 0;
        double numberOfScaledServices =0;
        double adaptationTime;  // metric for adaptability
        // For the same arrays of starting and ending points we are going to compute the perfscalefacor of throughtput
        for(int i=0; i< scaleRequestEndPoint.size(); i++){

            // for the starting and ending point of [i] computation of the maxThroughtput perfScaleFactors
            for(int j=0; j<prefixesOfMaxServices.length; j++){

                List<DataPoint> listBeforeMaxDapoint = client.QueryDataPointsAbsolute(prefixesOfMaxServices[j], new Date(0), scaleRequestStartPoint.get(i));
                maxBeforeScale = 0;
                for (int k = 0; k < listBeforeMaxDapoint.size(); k++) {
                    value = listBeforeMaxDapoint.get(k).getValue();
                    dvalue = Double.parseDouble(value.toString());
                    if(dvalue > maxBeforeScale){
                        timestampBeforeScale = listBeforeMaxDapoint.get(k).getTimestamp();
                        maxBeforeScale = dvalue;
                    }
                }



                List<DataPoint> listAfterMaxDapoint = client.QueryDataPointsAbsolute(prefixesOfMaxServices[i], scaleRequestEndPoint.get(i), null);
                maxAfterScale = 0;
                for (int k = 0; k < listAfterMaxDapoint.size(); k++) {
                    value = listAfterMaxDapoint.get(k).getValue();
                    dvalue = Double.parseDouble(value.toString());
                    if(dvalue > maxAfterScale){
                        timestampAfterScale = listAfterMaxDapoint.get(k).getTimestamp();
                        maxAfterScale = dvalue;}
                }


                timeToQualityRepair = timeToQualityRepair + (timestampAfterScale - timestampBeforeScale); // this will be used in order to calculate the MTTQR
                //push of the adaptation time of the according service
                client.putAlreadyInstantiateMetric(prefixesOfMaxServices[i].replace("MaxThroughput","AdaptationTime"), new Date().getTime(), timeToQualityRepair);
                if(maxBeforeScale > 0 && maxAfterScale > 0){
                    numberOfScaledServices++; // in case the we have the service being scaled, will be used in MTTQR
                    additionOfPerfFactors = additionOfPerfFactors + (maxAfterScale/maxBeforeScale);
                }
                else
                additionOfPerfFactors = 0;
            }

            double perfScaleFactor = additionOfPerfFactors/prefixesOfMaxServices.length;
            client.putAlreadyInstantiateMetric("PerfScaleFactor", new Date().getTime(), perfScaleFactor);
            additionOfPerfFactors = 0;
        }



        // calculation of the average scaling out procedure
        long startScaleOutDate;
        long endScaleOutDate;
        long scaleOutTime;
        long sumScaleOutTime = 0; // is used in the average precision of scaling out
        for(int i=0; i < scaleOutRequestEndPoint.size(); i++){
            startScaleOutDate = scaleOutRequestStartPoint.indexOf(i);
            endScaleOutDate = scaleOutRequestEndPoint.indexOf(i);
            scaleOutTime = endScaleOutDate - startScaleOutDate;
            sumScaleOutTime = sumScaleOutTime + scaleOutTime;
            client.putAlreadyInstantiateMetric("scaleOutTime", new Date().getTime(), scaleOutTime);
        }

        double averageScaleOutProcedure = 0;
        double sumScaleOutProcedure = 0;
        List<DataPoint> listScaleOutPoints = client.QueryDataPointsAbsolute("scaleOutTime", new Date(0), null);
        for(int j=0; j<listScaleOutPoints.size(); j++){
        Object scaleOutValue = listScaleOutPoints.get(j).getValue();
        Double dscaleOutValue = Double.valueOf(scaleOutValue.toString());
        sumScaleOutProcedure = sumScaleOutProcedure + dscaleOutValue;
        }
        if(listScaleOutPoints.size() > 0)
        averageScaleOutProcedure = sumScaleOutProcedure / listScaleOutPoints.size();
        client.putAlreadyInstantiateMetric("avgScalingOutTime", new Date().getTime(), averageScaleOutProcedure);

        // calculation of the average scaling in procedure
        long startScaleInDate;
        long endScaleInDate;
        long scaleInTime;
        long sumScaleInTime = 0; // is used in the average precision of scaling in
        for(int i=0; i < scaleInRequestEndPoint.size(); i++){
            startScaleInDate = scaleInRequestStartPoint.indexOf(i);
            endScaleInDate = scaleInRequestEndPoint.indexOf(i);
            scaleInTime = endScaleInDate - startScaleInDate;
            sumScaleInTime = sumScaleInTime + scaleInTime;
            client.putAlreadyInstantiateMetric("scaleInTime", new Date().getTime(), scaleInTime);
        }

        double averageScaleInProcedure = 0;
        double sumScaleInProcedure = 0;
        List<DataPoint> listScaleInPoints = client.QueryDataPointsAbsolute("scaleInTime", new Date(0), null);
        for(int j=0; j<listScaleInPoints.size(); j++){
            Object scaleInValue = listScaleInPoints.get(j).getValue();
            Double dscaleInValue = Double.valueOf(scaleInValue.toString());
            sumScaleInProcedure = sumScaleInProcedure + dscaleInValue;
        }
        if(listScaleInPoints.size() >0)
        averageScaleInProcedure = sumScaleInProcedure / listScaleInPoints.size();
        client.putAlreadyInstantiateMetric("avgScalingInTime", new Date().getTime(), averageScaleInProcedure);


        // push of the precision of scaling in and scaling out precision
        if(sumScaleOutTime > 0)
        client.putAlreadyInstantiateMetric("precisionScalingOut", new Date().getTime(), avgAmountOutProvisionedResources/sumScaleOutTime);
        if(sumScaleInTime > 0)
        client.putAlreadyInstantiateMetric("precisionScalingIn", new Date().getTime(), avgAmountInProvisionedResources/sumScaleInTime);

        // push the elasticity of scaling out and scaling in
        if(averageScaleOutProcedure * avgAmountOutProvisionedResources != 0)
        client.putAlreadyInstantiateMetric("elasticityScalingOut", new Date().getTime(),  1 / averageScaleOutProcedure * avgAmountOutProvisionedResources);
        if( averageScaleInProcedure * avgAmountInProvisionedResources != 0)
        client.putAlreadyInstantiateMetric("elasticityScalingIn", new Date().getTime(), 1 / averageScaleInProcedure * avgAmountInProvisionedResources);

        // push of the mean time to quality Repair
        double dmttqr = timeToQualityRepair / numberOfScaledServices;
        dmttqr = (dmttqr / 1000) / 60;
        client.putAlreadyInstantiateMetric("meanTimetoQualityRepair", new Date().getTime(), dmttqr);

        // push of the number of SLO violations that is taken from the external reporting system for
        // number of times that were given in scaleRequestStartPoint and scaleRequestEndPoint
        // for example if we have five such times the reporting sytem should give us the addition of fice SLO  violations
        // that represent these five time periods
        client.putAlreadyInstantiateMetric("NSOLV", new Date().getTime(), nsolv);
    }

    private void retrieveScalabilityAdaptabilityMetrics(KairosDbClient client, Date scalingPoint, ArrayList<String> serviceOfAdaptation, ArrayList<Double> preciseNumberOfAdaptationsScales, ArrayList<Double> totalTimesOfAdaptationScaling) throws Exception {

        String prefixesOfMaxServices [] = {"userMaxThroughput", "permAdminMaxThroughput", "objectMaxThroughput", "ouUserMaxThroughput", "roleMaxThroughput", "userDetailMaxThroughput", "permMaxThroughput", "objectAdminMaxThroughput", "roleAdminMaxThroughput"
                , "ouPermMaxThroughput", "sdDynamicMaxThroughput", "sdStaticMaxThroughput"};


        // before the scaling metrics
        double maxBeforeThroughtput = 0;
        for(int i=0; i< prefixesOfMaxServices.length; i++){

         maxBeforeThroughtput = 0; // variable of the max thoughput before the scaling
         long maxTimestamp = 0;
         String metricName;

        List<DataPoint> listDapoint = client.QueryDataPointsAbsolute(prefixesOfMaxServices[i], new Date(0), scalingPoint);
        System.out.println("DataPoints Size equals with : "  + listDapoint.size() + " name of metric equals with : " + prefixesOfMaxServices[i]);

            if(listDapoint.size() >0) {
                System.out.println("Size of it equals to : " + listDapoint.size());
                // here we take the values that we just get
                Long timestamp = listDapoint.get(0).getTimestamp();
                Long dtimestamp = Long.parseLong(timestamp.toString());
                Object value = listDapoint.get(0).getValue();
                Double dvalue = Double.parseDouble(value.toString());
                maxBeforeThroughtput = dvalue;
                maxTimestamp = dtimestamp;

                for (int j = 0; j< listDapoint.size(); j++) {
                    timestamp = listDapoint.get(j).getTimestamp();
                    dtimestamp = Long.parseLong(timestamp.toString());
                    value = listDapoint.get(j).getValue();
                    dvalue = Double.parseDouble(value.toString());
                    if( maxBeforeThroughtput < dvalue){
                        maxBeforeThroughtput = dvalue;
                        maxTimestamp = dtimestamp;
                    }
                }

                metricName = prefixesOfMaxServices[i].replace("MaxThroughput","");
                metricName = metricName + "ScalingUtilization";
                // push of the scaling utilization metric
                client.putAlreadyInstantiateMetric(metricName, maxTimestamp, maxBeforeThroughtput);
                metricName = metricName.replace("ScalingUtilization","ScalingRange");
                // push of the scaling range metric
                client.putAlreadyInstantiateMetric(metricName, maxTimestamp, maxBeforeThroughtput);
            }
        }

        double maxAfterThroughtput = 0; // variable of the max thoughput before the scaling
        // after the scaling metrics we just put the scaling point as the starting point
        for(int i=0; i< prefixesOfMaxServices.length; i++){

         maxAfterThroughtput = 0; // variable of the max thoughput before the scaling
           long maxTimestamp = 0;
           String metricName;

            List<DataPoint> listDapoint = client.QueryDataPointsAbsolute(prefixesOfMaxServices[i], scalingPoint, null);
            System.out.println("DataPoints Size equals with : "  + listDapoint.size() + " name of metric equals with : " + prefixesOfMaxServices[i]);

            if(listDapoint.size() >0) {
                System.out.println("Size of it equals to : " + listDapoint.size());
                // here we take the values that we just get
                Long timestamp = listDapoint.get(0).getTimestamp();
                Long dtimestamp = Long.parseLong(timestamp.toString());
                Object value = listDapoint.get(0).getValue();
                Double dvalue = Double.parseDouble(value.toString());
                maxAfterThroughtput = dvalue;
                maxTimestamp = dtimestamp;

                for (int j = 0; j< listDapoint.size(); j++) {
                    timestamp = listDapoint.get(j).getTimestamp();
                    dtimestamp = Long.parseLong(timestamp.toString());
                    value = listDapoint.get(j).getValue();
                    dvalue = Double.parseDouble(value.toString());
                    if( maxAfterThroughtput < dvalue){
                        maxAfterThroughtput = dvalue;
                        maxTimestamp = dtimestamp;
                    }
                }

                metricName = prefixesOfMaxServices[i].replace("MaxThroughput","");
                metricName = metricName + "ScalingSpeed";
                // push of the scaling ScalingSpeed metric
                client.putAlreadyInstantiateMetric(metricName, maxTimestamp, maxAfterThroughtput);
            }
        }

        // as many as are the number of the preciseNumberOfAdaptation the same number of totalTimesOfAdaptationScaling variable we are going to have
        // also totalTimesOfAdaptationScaling[0] and preciseNumberOfAdaptationsScales[0] equals to user service
        // totalTimesOfAdaptationScaling[1] and preciseNumberOfAdaptationsScales[1] equals to permAdmin service
        // so it follows the same sequence as the one of of the serviceAdaptation string table
        for(int i=0; i< preciseNumberOfAdaptationsScales.size(); i++){
        client.putAlreadyInstantiateMetric(serviceOfAdaptation.get(i)+"ScalingPrecision", new Date().getTime(), totalTimesOfAdaptationScaling.get(i)/preciseNumberOfAdaptationsScales.get(i));
        client.putAlreadyInstantiateMetric(serviceOfAdaptation.get(i)+"PrecisionOfAdaptation", new Date().getTime(), totalTimesOfAdaptationScaling.get(i)/preciseNumberOfAdaptationsScales.get(i));// here is one more metric for the adaptability action
        }
    }

}
