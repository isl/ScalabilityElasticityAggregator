package org.scalabilityelasticityaggreg;

/**
 * Copyright (C) 2015 KYRIAKOS KRITIKOS <kritikos@ics.forth.gr>
 * Modified by 2016 DAMIANOS METALLIDIS <metal@ics.forth.gr or metalidis07@gmail.com>
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/
 */


import org.kairosdb.client.HttpClient;
import org.kairosdb.client.builder.*;
import org.kairosdb.client.response.*;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.util.*;


public class KairosDbClient {
    private String url;
    private MetricBuilder fullMetricBuilder = MetricBuilder.getInstance();


    /**
     * @param Kairos DB url
     */
    public KairosDbClient(String url) {
        this.url = url;
    }

    private Metric copyMetric(Metric m1, Metric m2) throws Exception {
        for (DataPoint dp : m1.getDataPoints()) {
            System.out.println("Datapoint which is going to be written is with timestamp equals with " + dp.getTimestamp() + " and value " + dp.getValue());
            m2.addDataPoint(dp.getTimestamp(), dp.getValue());
        }
        if (m1.getTags() != null)
            for (Map.Entry<String, String> entry : m1.getTags().entrySet()) {
                System.out.println("The key of the tag is : " + entry.getKey() + " and the value is : " + entry.getValue());
                m2.addTag(entry.getKey(), entry.getValue());
            }

        return m2;
//        m2.addTags(m1.getTags());
    }

    /**
     * @param Insert a Metric m which is already instantiated
     *               MetricName, Tags and Values should not be null
     */
    public void putMetric(Metric m) throws Exception {
        MetricBuilder builder = MetricBuilder.getInstance();
        Metric m2 = builder.addMetric(m.getName(), m.getType());
        copyMetric(m, m2);

        HttpClient client = new HttpClient(url);
        try {
            Response response = client.pushMetrics(builder);
            if (response.getErrors().size() > 0) {
                for (String e : response.getErrors())
                    System.err.println("Response error: " + e);
            }
        } catch (URISyntaxException e) {
            System.err.println("PaaSage KairosDB Client : Error pushing metric, URI Syntax error");
            e.printStackTrace();
        } catch (IOException e) {
            System.err.println("PaaSage KairosDB Client : Error pushing metric, Io Exception");
            e.printStackTrace();
        }
        client.shutdown();

    }

    /**
     * Insert a Metric which is not instantiated
     * MetricName, Timestamp Tags and Values should not be null
     *
     * @param metricName
     * @param timestamp
     * @param value
     */
    public  Metric putMetric(String metricName, long timestamp, Object value) throws Exception {
        MetricBuilder builder = MetricBuilder.getInstance();
        addInFullBuilder(metricName, timestamp, value);
        System.out.println("Before metric with name " + metricName + " is going to be put for the first time its timestamp is : " + timestamp);
//        Date date = new Date(timestamp);
//        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z"); // the format of your date
//        sdf.setTimeZone(TimeZone.getTimeZone("GMT")); // give a timezone reference for formating (see comment at the bottom
//        String formattedDate = sdf.format(date);
//        System.out.println("Where in date format is : " + formattedDate + " \n");
        builder.addMetric(metricName)
                .addDataPoint(timestamp, value)
                .addTag("layer", "service");

        HttpClient client = new HttpClient(this.url);
        client.deleteMetric("userPage_up");
        client.deleteMetric("reliabilityUserPage_service");
        client.deleteMetric("reliabilityPermAdminPage_service");
        try {
            Response response = client.pushMetrics(builder);
            if (response.getErrors().size() > 0) {
                for (String e : response.getErrors())
                    System.err.println("Response error: " + e);
            }
        } catch (URISyntaxException e) {
            System.err.println("Cross-Layer Monitoring Framework KairosDB Client : Error pushing metric, URI Syntax error");
            e.printStackTrace();
        } catch (IOException e) {
            System.err.println("Cross-Layer Monitoring Framework KairosDB Client : Error pushing metric, Io Exception");
            e.printStackTrace();
        }
        client.shutdown();

        return builder.getMetrics().get(0);
    }

    /**
     * Insert a Metric which is not instantiated
     * MetricName, Timestamp Tags and Values should not be null
     *
     * @param metricName
     * @param timestamp
     * @param value
     */
    public Metric putMetric(String metricName, long timestamp) throws Exception {
        MetricBuilder builder = MetricBuilder.getInstance();
        addInFullBuilder(metricName, timestamp, null);
        System.out.println("Before metric with name " + metricName + " is going to be put for the first time its timestamp is : " + timestamp);
//        Date date = new Date(timestamp);
//        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z"); // the format of your date
//        sdf.setTimeZone(TimeZone.getTimeZone("GMT")); // give a timezone reference for formating (see comment at the bottom
//        String formattedDate = sdf.format(date);
//        System.out.println("Where in date format is : " + formattedDate + " \n");
        builder.addMetric(metricName)
                .addDataPoint(timestamp)
                .addTag("layer", "service");

        HttpClient client = new HttpClient(this.url);

        try {
            Response response = client.pushMetrics(builder);
            if (response.getErrors().size() > 0) {
                for (String e : response.getErrors())
                    System.err.println("Response error: " + e);
            }
        } catch (URISyntaxException e) {
            System.err.println("Cross-Layer Monitoring Framework KairosDB Client : Error pushing metric, URI Syntax error");
            e.printStackTrace();
        } catch (IOException e) {
            System.err.println("Cross-Layer Monitoring Framework KairosDB Client : Error pushing metric, Io Exception");
            e.printStackTrace();
        }
        client.shutdown();

        return builder.getMetrics().get(0);
    }


    // adds metrics to the full Builder
    private void addInFullBuilder(String metricName, double timestamp, Object value) {
        if (value != null)
            fullMetricBuilder.addMetric(metricName)
                    .addDataPoint((long) timestamp, value)
                    .addTag("layer", "service");
        else
            fullMetricBuilder.addMetric(metricName)
                    .addDataPoint((long) timestamp)
                    .addTag("layer", "service");
    }

    public boolean isMetricInstantiated(String metricName) throws IOException {

        HttpClient client = new HttpClient(url);
        GetResponse response = client.getMetricNames();

        for (String name : response.getResults()) {
            if (name.equals(metricName))
                return true;
        }

        return false;
    }

    /**
     * Lists all metrics stored in the KairosDB and returns an
     * ArrayList of their String representation
     *
     * @return ArrayList<String>
     */
    public ArrayList<String> ListAllMetrics() throws Exception {
        HttpClient client = new HttpClient(this.url);
        ArrayList<String> metricNames = new ArrayList<String>();
        GetResponse response;
        try {
            response = client.getMetricNames();
            //System.out.println("Response Code =" + response.getStatusCode());
            for (String name : response.getResults())
                metricNames.add(name);
        } catch (IOException e) {
            System.err.println("PaaSage TSDB Client Error Listing metic names " + e);
        }
        client.shutdown();
        return metricNames;

    }

    /**
     * Lists all the tag names stored in the KairosDb and returns an
     * ArrayList of their String representation
     *
     * @return ArrayList<String>
     */
    public ArrayList<String> ListAllTags() throws Exception {
        HttpClient client = new HttpClient(this.url);
        ArrayList<String> metricTags = new ArrayList<String>();
        GetResponse response;
        try {
            response = client.getTagNames();
            //System.out.println("Response Code =" + response.getStatusCode());
            for (String name : response.getResults())
                metricTags.add(name);
        } catch (IOException e) {
            System.err.println("PaaSage TSDB Client Error Listing Tag names " + e);
            e.printStackTrace();
        }
        client.shutdown();
        return metricTags;

    }

    /**
     * Querying data points is similarly done by using the QueryBuilder class. A query requires a date range. The start date is
     * required, but the end date defaults to NOW if not specified. The metric(s) that you are querying for is also required.
     * Optionally, tags may be added to narrow down the search.
     * <p>
     * * This Query Builder is used with Absolute Dates
     * for example from now till 2 days ago
     *
     * @param metric
     * @param start
     * @param end
     * @param unit
     * @return
     */
    public List<DataPoint> QueryDataPoints(String metric, int start, int end, TimeUnit unit) throws Exception {
        QueryBuilder builder = QueryBuilder.getInstance();

        if (start != -1 && end != -1 && end > start) {
            System.err.print("Start Date should be greater than End Date");
            return null;
        }

        builder.setStart(start, unit)
                .addMetric(metric);
        if (end != -1) builder.setEnd(end, unit);

        HttpClient client = new HttpClient(this.url);
        try {
            QueryResponse response = client.query(builder);
            for (Queries q : response.getQueries()) {
                //System.out.println("For result R "+ q.getResults());
                Iterator<Results> it = q.getResults().iterator();
                while (it.hasNext()) {
                    Results tmp = it.next();
                    System.out.println("Got Result " + tmp.getName());
                    System.out.println("Data Points List: " + tmp.getDataPoints());
                    return tmp.getDataPoints();
                }
            }
        } catch (URISyntaxException e) {
            System.err.println("Cross-Layer Monitoring Framework KairosDB Error QueryDataPoints " + e);
            e.printStackTrace();
        } catch (IOException e) {
            System.err.println("Cross-Layer Monitoring Framework KairosDB Error QueryDataPoints " + e);
            e.printStackTrace();
        }
        client.shutdown();
        return null;
    }

	/*
	 * Querying data points is similarly done by using the QueryBuilder class. A query requires a date range. The start date is
	 * required, but the end date defaults to NOW if not specified. The metric(s) that you are querying for is also required.
	 * Optionally, tags may be added to narrow down the search.
	 *
	 * This Query Builder is used with Absolute Dates
	 * for example from 12/3/2014 to 12/4/2014
	 */

    /**
     * @param metric
     * @param start
     * @param end
     * @return
     */
    public List<DataPoint> QueryDataPointsAbsolute(String metric, Date start, Date end) throws Exception {
        QueryBuilder builder = QueryBuilder.getInstance();
        if (end != null)
            builder.setStart(start)
                    .setEnd(end)
                    .addMetric(metric);
        else {
            builder.setStart(start)
                    .addMetric(metric);
        }

        HttpClient client = new HttpClient(this.url);
        try {
            QueryResponse response = client.query(builder);
            for (Queries q : response.getQueries()) {
                System.out.println("For result R " + q.getResults().size());
                Iterator<Results> it = q.getResults().iterator();
                while (it.hasNext()) {
                    Results tmp = it.next();
//                    System.out.println("Got Result "+ tmp.getName());
//                    System.out.println("Data Points List: " + tmp.getDataPoints().size());
//                    System.out.println("The timestamp is : " + new Date((tmp.getDataPoints().get(0).getTimestamp())).toString());
                    return tmp.getDataPoints();
                }
            }
        } catch (URISyntaxException e) {
            System.err.println("Cross-Layer Monitoring Framework KairosDB Error QueryDataPoints " + e);
            e.printStackTrace();
        } catch (IOException e) {
            System.err.println("Cross-Layer Monitoring Framework KairosDB Error QueryDataPoints " + e);
            e.printStackTrace();
        }
        client.shutdown();
        return null;
    }

    /**
     * Same as Relative Query Builder plus the aggregator instance
     *
     * @param metric
     * @param start
     * @param end
     * @param unit
     * @param ag
     * @return
     */
    public List<DataPoint> QueryAggregatedDataPoints(String metric, int start, int end, TimeUnit unit, Aggregator ag) throws Exception {
        QueryBuilder builder = QueryBuilder.getInstance();


        if (start != -1 && end != -1 && end > start) {
            System.err.print("Start Date should be greater than End Date");
            return null;
        }

        builder.setStart(start, unit)
                .addMetric(metric)
                .addAggregator(ag);
        if (end != -1) builder.setEnd(end, unit);

        HttpClient client = new HttpClient(this.url);
        try {
            QueryResponse response = client.query(builder);
            for (Queries q : response.getQueries()) {
                //System.out.println("For result R "+ q.getResults());
                Iterator<Results> it = q.getResults().iterator();
                while (it.hasNext()) {
                    Results tmp = it.next();
//					System.out.println("Got Result "+ tmp.getName());
//					System.out.println("Data Points List: "+ tmp.getDataPoints());
                    return tmp.getDataPoints();
                }
            }
        } catch (URISyntaxException e) {
            System.err.println("Cross-Layer Monitoring Framework KairosDB Error QueryDataPoints " + e);
            e.printStackTrace();
        } catch (IOException e) {
            System.err.println("Cross-Layer Monitoring Framework KairosDB Error QueryDataPoints " + e);
            e.printStackTrace();
        }
        client.shutdown();
        return null;
    }


    public void deleteMetrics() throws IOException {

        HttpClient client = new HttpClient(this.url);
        client.deleteMetric("acknowledgedMessages");
        client.deleteMetric("requestMessages");
        client.deleteMetric("responseMessages");
//        client.deleteMetric("reliabilityUserPage_service");
//        client.deleteMetric("reliabilityUserPage_service1");
//        client.deleteMetric("reliabilityUserPage_service3");
//
//        client.deleteMetric("userPage_Down1");
//        client.deleteMetric("userPage_Down11");
//        client.deleteMetric("userPage_Down12");
//
//        client.deleteMetric("userPage_Up11");
//        client.deleteMetric("userPage_Up12");
//        client.deleteMetric("userPage_Up");

    }


    public static void main(String[] args) throws Exception {


         KairosDbClient dbclient = new KairosDbClient("http://localhost:8088");
     //   KairosDbClient dbclient = new KairosDbClient("http://147.52.82.63:8088/");

        dbclient.printAvgMetricsDatapointsServiceInfra(dbclient);


    //    dbclient.deleteAllMetrics(dbclient);

    }

    private void printAvgMetricsDatapointsServiceInfra(KairosDbClient dbclient) throws Exception {

        ArrayList<String> allMetrics = dbclient.ListAllMetrics();

        for (int i =0; i< allMetrics.size(); i++){


                String metricName = allMetrics.get(i);
            System.out.println("For the metric of "  + metricName);
            if(!(metricName.contains("kairosdb")) && !(metricName.contains("workflowMonitoring"))) {
                List<DataPoint> listDapoint = dbclient.QueryDataPointsAbsolute(metricName, new Date(0), null);
                Object value;
                double dvalue;
                double dsum = 0;

                if (listDapoint.size() > 0) {
                    for (int j = 0; j < listDapoint.size(); j++) {
                        value = listDapoint.get(j).getValue();
                        dvalue = Double.parseDouble(value.toString());
                        dsum = dsum + dvalue;
                    }
                    System.out.println("Metric with name:" + metricName + " has values of  : " + dsum / listDapoint.size());
                }

            }
        }
    }

    private  void printAvgMetricsDatapoints(KairosDbClient dbclient) throws Exception {

        ArrayList<String> allMetrics = dbclient.ListAllMetrics();

        for (int i =0; i< allMetrics.size(); i++){

            if(allMetrics.get(i).contains("workflowMonitoring")){

                String metricName = allMetrics.get(i);
                List<DataPoint> listDapoint = dbclient.QueryDataPointsAbsolute(metricName, new Date(0), null);
                Object value;
                double dvalue;
                double dsum = 0;

                if ( listDapoint.size() > 0){
                   for(int j =0; j<listDapoint.size(); j++)
                   {
                       value = listDapoint.get(j).getValue();
                       dvalue =  Double.parseDouble(value.toString());
                       dsum = dsum + dvalue;
                   }
                    System.out.println("Metric with name:" + metricName + " has values of  : " + dsum/listDapoint.size());
                }

            }


        }

    }

    public  void putAlreadyInstantiateMetric(String metricName, long unixTime, Object milli) throws Exception {

        if (isMetricInstantiated(metricName)) {
            List<Metric> metrics = fullMetricBuilder.getMetrics();
            MetricBuilder builder = MetricBuilder.getInstance();
            Metric initializedMetric = null;
            // tha skasei se periptwsh poy to fullMetricBuilder einai empty kai ayto tha ginei otan gia paradigma stamathsoume kai ksanatreksoyme
            // ton aggregator
            for (Metric m : metrics) {
                if (m.getName().equals(metricName)) {
                    initializedMetric = m;
                    break;
                }
            }

            //oti eixe to palio(poy yperxe hdh ta vazeis sto kainourio) ta vazeis sto kainourio
            System.out.println("UnixTime that is going to be put is :" + unixTime + " and milliseconds are : " + milli);
            Metric updatedDataPointsMetric = builder.addMetric(metricName);
            initializedMetric = copyMetric(initializedMetric, updatedDataPointsMetric);
            // kai meta vazeis kai to kainoyrio datapoint
            initializedMetric.addDataPoint(unixTime, milli);
            metrics.add(initializedMetric);


            HttpClient client = new HttpClient(url);
            try {
                Response response = client.pushMetrics(builder);
                if (response.getErrors().size() > 0) {
                    for (String e : response.getErrors())
                        System.err.println("Response error: " + e);
                }
            } catch (URISyntaxException e) {
                System.err.println("Cross-Layer Monitoring Framework KairosDB Client : Error pushing metric, URI Syntax error");
                e.printStackTrace();
            } catch (IOException e) {
                System.err.println("Cross-Layer Monitoring Framework KairosDB Client : Error pushing metric, Io Exception");
                e.printStackTrace();
            }
            client.shutdown();
        } else {
            putMetric(metricName, unixTime, milli);
        }
    }


    public void deleteAllMetrics() throws IOException {
        String rawMetricNames[] = {"userRequestCompletionTime_latency_milliseconds", "userProcess_latency_milliseconds",
                "userDelay_latency_milliseconds", "userAnswerDelay_latency_milliseconds", "userNetworkLatency_latency_milliseconds",

                "permAdminRequestCompletionTime_latency_milliseconds", "permAdminProcess_latency_milliseconds",
                "permAdminDelay_latency_milliseconds", "permAdminAnswerDelay_latency_milliseconds", "permAdminNetworkLatency_latency_milliseconds",

                "objectRequestCompletionTime_latency_milliseconds", "objectProcess_latency_milliseconds",
                "objectDelay_latency_milliseconds", "objectAnswerDelay_latency_milliseconds", "objectNetworkLatency_latency_milliseconds",

                "ouUserRequestCompletionTime_latency_milliseconds", "ouUserProcess_latency_milliseconds",
                "ouUserDelay_latency_milliseconds", "ouUserAnswerDelay_latency_milliseconds", "ouUserNetworkLatency_latency_milliseconds",

                "roleRequestCompletionTime_latency_milliseconds", "roleProcess_latency_milliseconds",
                "roleDelay_latency_milliseconds", "roleAnswerDelay_latency_milliseconds", "roleNetworkLatency_latency_milliseconds",

                "userDetailRequestCompletionTime_latency_milliseconds", "userDetailProcess_latency_milliseconds",
                "userDetailDelay_latency_milliseconds", "userDetailAnswerDelay_latency_milliseconds", "userDetailNetworkLatency_latency_milliseconds",

                "permRequestCompletionTime_latency_milliseconds", "permProcess_latency_milliseconds",
                "permDelay_latency_milliseconds", "permAnswerDelay_latency_milliseconds", "permNetworkLatency_latency_milliseconds",

                "objectAdminRequestCompletionTime_latency_milliseconds", "objectAdminProcess_latency_milliseconds",
                "objectAdminDelay_latency_milliseconds", "objectAdminAnswerDelay_latency_milliseconds", "objectAdminNetworkLatency_latency_milliseconds",

                "roleAdminRequestCompletionTime_latency_milliseconds", "roleAdminProcess_latency_milliseconds",
                "roleAdminDelay_latency_milliseconds", "roleAdminAnswerDelay_latency_milliseconds", "roleAdminNetworkLatency_latency_milliseconds",

                "ouPermRequestCompletionTime_latency_milliseconds", "ouPermProcess_latency_milliseconds",
                "ouPermDelay_latency_milliseconds", "ouPermAnswerDelay_latency_milliseconds", "ouPermNetworkLatency_latency_milliseconds",

                "sdDynamicRequestCompletionTime_latency_milliseconds", "sdDynamicProcess_latency_milliseconds",
                "sdDynamicDelay_latency_milliseconds", "sdDynamicAnswerDelay_latency_milliseconds", "sdDynamicNetworkLatency_latency_milliseconds",

                "sdStaticRequestCompletionTime_latency_milliseconds", "sdStaticProcess_latency_milliseconds",
                "sdStaticDelay_latency_milliseconds", "sdStaticAnswerDelay_latency_milliseconds", "sdStaticNetworkLatency_latency_milliseconds"
        };

        String compositeMetricNames[] = {"userExecutionTime_latency_milliseconds", "userRawResponseTime_latency_milliseconds",
                "permAdminExecutionTime_latency_milliseconds", "permAdminRawResponseTime_latency_milliseconds",
                "objectExecutionTime_latency_milliseconds", "objectRawResponseTime_latency_milliseconds",
                "ouUserExecutionTime_latency_milliseconds", "ouUserRawResponseTime_latency_milliseconds",
                "roleExecutionTime_latency_milliseconds", "roleRawResponseTime_latency_milliseconds",
                "userDetailExecutionTime_latency_milliseconds", "userDetailRawResponseTime_latency_milliseconds",
                "permExecutionTime_latency_milliseconds", "permRawResponseTime_latency_milliseconds",
                "objectAdminExecutionTime_latency_milliseconds", "objectAdminRawResponseTime_latency_milliseconds",
                "roleAdminExecutionTime_latency_milliseconds", "roleAdminRawResponseTime_latency_milliseconds",
                "ouPermExecutionTime_latency_milliseconds", "ouPermRawResponseTime_latency_milliseconds",
                "sdDynamicExecutionTime_latency_milliseconds", "sdDynamicRawResponseTime_latency_milliseconds",
                "sdStaticExecutionTime_latency_milliseconds", "sdStaticRawResponseTime_latency_milliseconds",
        };

        String prefixesOfAvailabilityServices [] = {"userPage_service_availabilityPercentage", "permAdminPage_service_availabilityPercentage", "objectPage_service_availabilityPercentage", "ouUserPage_service_availabilityPercentage", "rolePage_service_availabilityPercentage", "userDetailPage_service_availabilityPercentage", "permPage_service_availabilityPercentage", "objectAdminPage_service_availabilityPercentage", "roleAdminPage_service_availabilityPercentage"
                , "ouPermPage_service_availabilityPercentage", "sdDynamicPage_service_availabilityPercentage", "sdStaticPage_service_availabilityPercentage"};

        String prefixesOfDownServices [] = {"userPage_service_downPercentage", "permAdminPage_service_downPercentage", "objectPage_service_downPercentage", "ouUserPage_service_downPercentage", "rolePage_service_downPercentage", "userDetailPage_service_downPercentage", "permPage_service_downPercentage", "objectAdminPage_service_downPercentage", "roleAdminPage_service_downPercentage"
                , "ouPermPage_service_downPercentage", "sdDynamicPage_service_downPercentage", "sdStaticPage_service_downPercentage"};

        String prefixesOfAvgServices [] = {"userAvgThroughput", "permAdminAvgThroughput", "objectAvgThroughput", "ouUserAvgThroughput", "roleAvgThroughput", "userDetailAvgThroughput", "permAvgThroughput", "objectAdminAvgThroughput", "roleAdminAvgThroughput"
                , "ouPermAvgThroughput", "sdDynamicAvgThroughput", "sdStaticAvgThroughput"};

        String prefixesOfMaxServices [] = {"userMaxThroughput", "permAdminMaxThroughput", "objectMaxThroughput", "ouUserMaxThroughput", "roleMaxThroughput", "userDetailMaxThroughput", "permMaxThroughput", "objectAdminMaxThroughput", "roleAdminMaxThroughput"
                , "ouPermMaxThroughput", "sdDynamicMaxThroughput", "sdStaticMaxThroughput"};

        String prefixesOfOtherMetrics [] = {"mttf", "responseMessages", "requestMessages", "acknowledgedMessages", "failedAuthentications", "totalAttemptsOfAuthentication"};

        String prefixesOfMaxMilliServices [] = {"userRawResponseTime_latency_milliseconds_max", "permAdminRawResponseTime_latency_milliseconds_max", "objectRawResponseTime_latency_milliseconds_max", "ouUserRawResponseTime_latency_milliseconds_max", "roleRawResponseTime_latency_milliseconds_max", "userDetailRawResponseTime_latency_milliseconds_max", "permRawResponseTime_latency_milliseconds_max", "objectAdminRawResponseTime_latency_milliseconds_max", "roleAdminRawResponseTime_latency_milliseconds_max"
                , "ouPermRawResponseTime_latency_milliseconds_max", "sdDynamicRawResponseTime_latency_milliseconds_max", "sdStaticRawResponseTime_latency_milliseconds_max"};

        int i;

        HttpClient client = new HttpClient(this.url);
        for(i=0; i<rawMetricNames.length; i++)
            client.deleteMetric(rawMetricNames[i]);
        for(i=0; i<compositeMetricNames.length; i++)
            client.deleteMetric(compositeMetricNames[i]);
        for(i=0; i<prefixesOfAvailabilityServices.length; i++)
            client.deleteMetric(prefixesOfAvailabilityServices[i]);
        for(i=0; i<prefixesOfDownServices.length; i++)
            client.deleteMetric(prefixesOfDownServices[i]);
        for(i=0; i<prefixesOfAvgServices.length; i++)
            client.deleteMetric(prefixesOfAvgServices[i]);
        for(i=0; i<prefixesOfMaxServices.length; i++)
            client.deleteMetric(prefixesOfMaxServices[i]);
        for(i=0; i<prefixesOfOtherMetrics.length; i++)
            client.deleteMetric(prefixesOfOtherMetrics[i]);
        for(i=0; i<prefixesOfMaxMilliServices.length; i++)
            client.deleteMetric(prefixesOfMaxMilliServices[i]);
    }


    public void deleteAllMetrics(KairosDbClient dbclient) throws Exception {
        ArrayList<String> arrayMetrics = dbclient.ListAllMetrics();
        int i;

        HttpClient client = new HttpClient(this.url);
        for(i=0; i<arrayMetrics.size(); i++){
            System.out.println("The metric is named after : " + arrayMetrics.get(i));
            String metric = URLEncoder.encode(arrayMetrics.get(i), "UTF-8");
            //metric.replaceAll(" ", "%20");
            if(!metric.contains("failureRate") || !metric.contains("availability") || !metric.contains("exec") || !metric.contains("ProcessTime") || !metric.contains("TransitionDelayTime"))
                client.deleteMetric(metric);
        }
    }



    public KairosDbClient initializeFullBuilder(KairosDbClient client) throws Exception {

        ArrayList<String> allMetrics = client.ListAllMetrics();

        for(int i=0; i<allMetrics.size(); i++){

            if(!(allMetrics.get(i).contains("kairosdb"))){

                List<DataPoint> listDatapoints = client.QueryDataPointsAbsolute(allMetrics.get(i), new Date(0), null);
                if(listDatapoints.size() >0){

                    for (int j = 0; j < listDatapoints.size(); j++) {
                        Long timestamp = listDatapoints.get(j).getTimestamp();
                        Object value = listDatapoints.get(j).getValue();
                        Double dvalue = Double.parseDouble(value.toString());
                        client.addInFullBuilder(allMetrics.get(i), timestamp, dvalue); // add in full metric builder
                    }
                }

            }
        }
        return client;

    }
}