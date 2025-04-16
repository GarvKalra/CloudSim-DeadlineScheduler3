package com.java;

import org.cloudbus.cloudsim.*;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.provisioners.*;

import java.text.DecimalFormat;
import java.util.*;

public class GreenCloudSimExample3_04 {
    private static final int CLOUDLET_NUM = 200;
    private static final DecimalFormat df = new DecimalFormat("#.###");

    public static void main(String[] args) {
        try {
            Map<String, Map<Integer, ResultMetrics>> allResults = new LinkedHashMap<>();

            for (String algo : new String[]{"RR", "SJF", "FCFS"}) {
                allResults.put(algo, new LinkedHashMap<>());
                for (int vmCount : new int[]{3, 5}) {
                    ResultMetrics metrics = runSimulation(algo, vmCount, 3);
                    allResults.get(algo).put(vmCount, metrics);
                }
            }

            printFinalReport(allResults);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static ResultMetrics runSimulation(String algo, int vmNum, int hostNum) throws Exception {
        CloudSim.init(1, Calendar.getInstance(), false);
        EnergyAwareDatacenter datacenter = createDatacenter("Datacenter_" + algo, hostNum);
        DatacenterBroker broker = new DatacenterBroker("Broker");

        int brokerId = broker.getId();
        List<Vm> vmList = createVMs(brokerId, vmNum, algo);
        List<Cloudlet> cloudletList = createCloudlets(brokerId);

        if (algo.equals("SJF"))
            cloudletList.sort(Comparator.comparingLong(Cloudlet::getCloudletLength));
        else if (algo.equals("FCFS"))
            cloudletList.sort(Comparator.comparingInt(Cloudlet::getCloudletId));
        else if (algo.equals("RR")) {
            for (int i = 0; i < cloudletList.size(); i++) {
                cloudletList.get(i).setVmId(vmList.get(i % vmList.size()).getId());
            }
        }

        broker.submitVmList(vmList);
        broker.submitCloudletList(cloudletList);

        CloudSim.startSimulation();
        List<Cloudlet> finishedList = broker.getCloudletReceivedList();
        CloudSim.stopSimulation();

        double makespan = 0, totalExecTime = 0;
        for (Cloudlet cl : finishedList) {
            if (cl.getStatus() == Cloudlet.SUCCESS) {
                makespan = Math.max(makespan, cl.getFinishTime());
                totalExecTime += cl.getActualCPUTime();
            }
        }

        ResultMetrics metrics = new ResultMetrics();
        metrics.energy = datacenter.getTotalEnergy();
        metrics.makespan = makespan;
        metrics.avgExecTime = totalExecTime / finishedList.size();
        metrics.hostEnergies = datacenter.hostEnergyLog;

        printCloudletList(finishedList, algo, vmNum, metrics);
        return metrics;
    }

    private static List<Vm> createVMs(int userId, int vmNum, String algo) {
        List<Vm> vms = new ArrayList<>();
        for (int i = 0; i < vmNum; i++) {
            CloudletScheduler scheduler = algo.equals("RR") ?
                    new CloudletSchedulerTimeShared() :
                    new CloudletSchedulerSpaceShared();

            Vm vm = new Vm(i, userId, 1000 + i * 100, 1, 1024, 1000, 10000, "Xen", scheduler);
            vms.add(vm);
        }
        return vms;
    }

    private static List<Cloudlet> createCloudlets(int userId) {
        List<Cloudlet> cloudlets = new ArrayList<>();
        int[] lengths = {6000, 20000, 8000, 10000, 120000, 6000, 70000, 9000, 40000, 30000};
        for (int i = 0; i < CLOUDLET_NUM; i++) {
            int len = lengths[i % lengths.length];
            UtilizationModel util = new UtilizationModelStochastic();
            Cloudlet cl = new Cloudlet(i, len, 1, 300, 300, util, util, util);
            cl.setUserId(userId);
            cloudlets.add(cl);
        }
        return cloudlets;
    }

    private static EnergyAwareDatacenter createDatacenter(String name, int hostNum) throws Exception {
        List<Host> hostList = new ArrayList<>();
        for (int i = 0; i < hostNum; i++) {
            List<Pe> peList = new ArrayList<>();
            for (int j = 0; j < 4; j++) peList.add(new Pe(j, new PeProvisionerSimple(2000)));

            Host host = new Host(i,
                    new RamProvisionerSimple(8192),
                    new BwProvisionerSimple(10000),
                    1000000,
                    peList,
                    new VmSchedulerTimeShared(peList)
            );
            hostList.add(host);
        }

        DatacenterCharacteristics characteristics = new DatacenterCharacteristics(
                "x86", "Linux", "Xen", hostList, 10.0, 3.0, 0.05, 0.001, 0.1
        );

        return new EnergyAwareDatacenter(name, characteristics,
                new VmAllocationPolicySimple(hostList), new LinkedList<>(), 1.0);
    }

    private static void printCloudletList(List<Cloudlet> list, String algo, int vmNum, ResultMetrics metrics) {
        System.out.println("\n=== " + algo + " with " + vmNum + " VMs ===");
        System.out.println("ID\tSTATUS\tVM ID\tTime (s)\tStart\tFinish");
        for (Cloudlet cl : list) {
            if (cl.getStatus() == Cloudlet.SUCCESS) {
                System.out.println(cl.getCloudletId() + "\tSUCCESS\t" +
                        cl.getVmId() + "\t" + df.format(cl.getActualCPUTime()) + "\t" +
                        df.format(cl.getExecStartTime()) + "\t" + df.format(cl.getFinishTime()));
            }
        }

        System.out.println("Energy Used: " + df.format(metrics.energy) + " kWh");
        System.out.println("Makespan: " + df.format(metrics.makespan) + " seconds");
        System.out.println("Avg Exec Time: " + df.format(metrics.avgExecTime) + " seconds");
        System.out.println("-- Host-wise Energy --");
        for (Map.Entry<Integer, Double> entry : metrics.hostEnergies.entrySet()) {
            System.out.println("Host " + entry.getKey() + ": " + df.format(entry.getValue()) + " kWh");
        }
    }

    private static void printFinalReport(Map<String, Map<Integer, ResultMetrics>> results) {
        System.out.println("\n==== FINAL COMPARISON REPORT ====");
        System.out.printf("%-10s %-5s %-15s %-15s %-20s\n",
                "Algorithm", "VMs", "Energy (kWh)", "Makespan (s)", "Avg Exec Time (s)");
        System.out.println("---------------------------------------------------------------");

        for (String algo : results.keySet()) {
            for (int vm : results.get(algo).keySet()) {
                ResultMetrics m = results.get(algo).get(vm);
                System.out.printf("%-10s %-5d %-15s %-15s %-20s\n",
                        algo,
                        vm,
                        df.format(m.energy),
                        df.format(m.makespan),
                        df.format(m.avgExecTime));
            }
        }
    }

    static class ResultMetrics {
        double energy;
        double makespan;
        double avgExecTime;
        Map<Integer, Double> hostEnergies = new HashMap<>();
    }

    static class EnergyAwareDatacenter extends Datacenter {
        private double totalEnergy = 0.0;
        private double lastTime = 0.0;
        private final double idlePower = 100.0;
        private final double maxPower = 250.0;
        Map<Integer, Double> hostEnergyLog = new HashMap<>();

        public EnergyAwareDatacenter(String name, DatacenterCharacteristics characteristics,
                                     VmAllocationPolicy vmAllocationPolicy, List<Storage> storageList,
                                     double schedulingInterval) throws Exception {
            super(name, characteristics, vmAllocationPolicy, storageList, schedulingInterval);
        }

        @Override
        protected void updateCloudletProcessing() {
            double currentTime = CloudSim.clock();
            double timeDiff = currentTime - lastTime;

            if (timeDiff > 0) {
                for (Host host : getHostList()) {
                    double util = getHostUtilization(host);

                    // LINEAR power model: P = idle + (max - idle) * utilization
                    double power = idlePower + (maxPower - idlePower) * util;
                    double energyUsed = (power * timeDiff) / 3600.0;

                    totalEnergy += energyUsed;
                    hostEnergyLog.put(host.getId(), hostEnergyLog.getOrDefault(host.getId(), 0.0) + energyUsed);
                }
                lastTime = currentTime;
            }

            super.updateCloudletProcessing();
        }

        private double getHostUtilization(Host host) {
            double totalMips = 0, usedMips = 0;
            for (Pe pe : host.getPeList()) totalMips += pe.getMips();
            for (Vm vm : host.getVmList())
                usedMips += host.getVmScheduler().getAllocatedMipsForVm(vm)
                        .stream().mapToDouble(Double::doubleValue).sum();
            return (totalMips == 0) ? 0 : usedMips / totalMips;
        }

        public double getTotalEnergy() {
            return totalEnergy;
        }
    }
}

