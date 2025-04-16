package com.java;

import org.cloudbus.cloudsim.*;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.provisioners.*;
import java.util.*;

public class DeadlineAwareSimulation {

    public static void main(String[] args) throws Exception {
        int numUsers = 1;
        Calendar calendar = Calendar.getInstance();
        boolean traceFlag = false;
        int VMs=200;
        int cloudlets=500;	
        CloudSim.init(numUsers, calendar, traceFlag);

        Datacenter datacenter = createDatacenter("Datacenter_1");

        DeadlineAwareBroker broker = new DeadlineAwareBroker("DeadlineBroker");
        List<Vm> vmlist = new ArrayList<>();
        List<Cloudlet> cloudletList = new ArrayList<>();

        // VMs
        for (int i = 0; i < VMs; i++) {
            Vm vm = new Vm(i, broker.getId(), 1000, 1, 2048, 1000, 10000,
                    "Xen", new CloudletSchedulerTimeShared());
            vmlist.add(vm);
        }

        // Deadline Cloudlets
        for (int i = 0; i < cloudlets; i++) {
            DeadlineCloudlet cloudlet = new DeadlineCloudlet(
                    i, 10000, 1, 300, 300,
                    new UtilizationModelFull(), new UtilizationModelFull(), new UtilizationModelFull(),
                    50 + i * 10 // Assigning increasing deadlines
            );
            cloudlet.setUserId(broker.getId());
            cloudletList.add(cloudlet);
        }

        // Sort cloudlets by deadlines (earliest deadline first)
        cloudletList.sort(Comparator.comparingDouble(c -> ((DeadlineCloudlet) c).getDeadline()));

        broker.submitVmList(vmlist);
        broker.submitCloudletList(cloudletList);

        // Enhanced logic for assigning cloudlets based on VM load and deadlines
        assignCloudletsToVMs(broker, cloudletList, vmlist);

        CloudSim.startSimulation();

        List<Cloudlet> newList = broker.getCloudletReceivedList();
        CloudSim.stopSimulation();

        System.out.println("\n========== Results ==========");
        for (Cloudlet cloudlet : newList) {
            DeadlineCloudlet dCloudlet = (DeadlineCloudlet) cloudlet;
            double deadline = dCloudlet.getDeadline();
            double finishTime = dCloudlet.getFinishTime();
            boolean metDeadline = finishTime <= deadline;

            System.out.println("Cloudlet ID: " + dCloudlet.getCloudletId()
                    + " | VM: " + dCloudlet.getVmId()
                    + " | Status: " + dCloudlet.getStatus()
                    + " | Deadline: " + deadline
                    + " | Finish Time: " + finishTime
                    + " | " + (metDeadline ? "✅ Met Deadline" : "❌ Missed Deadline"));
        }
    }

    private static void assignCloudletsToVMs(DeadlineAwareBroker broker, List<Cloudlet> cloudletList, List<Vm> vmlist) {
        Map<Integer, List<Cloudlet>> vmLoads = new HashMap<>();
        for (Vm vm : vmlist) {
            vmLoads.put(vm.getId(), new ArrayList<>());
        }

        for (Cloudlet cloudlet : cloudletList) {
            Vm selectedVm = null;
            long earliestFinishTime = Long.MAX_VALUE;

            for (Vm vm : vmlist) {
                List<Cloudlet> assignedCloudlets = vmLoads.get(vm.getId());
                long vmFinishTime = getEstimatedFinishTime(assignedCloudlets, cloudlet, vm);
                if (vmFinishTime < earliestFinishTime) {
                    earliestFinishTime = vmFinishTime;
                    selectedVm = vm;
                }
            }

            if (selectedVm != null) {
                vmLoads.get(selectedVm.getId()).add(cloudlet);
                broker.bindCloudletToVm(cloudlet.getCloudletId(), selectedVm.getId());
            }
        }
    }

    private static long getEstimatedFinishTime(List<Cloudlet> assignedCloudlets, Cloudlet newCloudlet, Vm vm) {
        long totalLength = 0;
        for (Cloudlet cl : assignedCloudlets) {
            totalLength += cl.getCloudletLength();
        }
        totalLength += newCloudlet.getCloudletLength();
        return (long) (totalLength / vm.getMips());
    }

    private static Datacenter createDatacenter(String name) throws Exception {
        List<Host> hostList = new ArrayList<>();
        
        int numHosts = 20; // Adjust depending on needs
        int pePerHost = 8; // 8 cores per host

        for (int i = 0; i < numHosts; i++) {
            List<Pe> peList = new ArrayList<>();
            for (int j = 0; j < pePerHost; j++) {
                peList.add(new Pe(j, new PeProvisionerSimple(2000))); // Each PE has 2000 MIPS
            }

            Host host = new Host(
                    i,
                    new RamProvisionerSimple(32768),     // 32 GB RAM
                    new BwProvisionerSimple(10000),
                    1000000,                              // Storage
                    peList,
                    new VmSchedulerTimeShared(peList)
            );

            hostList.add(host);
        }

        DatacenterCharacteristics characteristics = new DatacenterCharacteristics(
                "x86", "Linux", "Xen", hostList,
                10.0, 3, 0.05, 0.1, 0.1);

        return new Datacenter(name, characteristics,
                new VmAllocationPolicySimple(hostList),
                new LinkedList<Storage>(), 0);
    }


    // Inner class: Deadline-Aware Broker
    public static class DeadlineAwareBroker extends DatacenterBroker {
        public DeadlineAwareBroker(String name) throws Exception {
            super(name);
        }

        @Override
        protected void submitCloudlets() {
            getCloudletList().sort(Comparator.comparingDouble(c -> ((DeadlineCloudlet) c).getDeadline()));
            super.submitCloudlets();
        }
    }

    // Inner class: Deadline Cloudlet
    public static class DeadlineCloudlet extends Cloudlet {
        private double deadline;

        public DeadlineCloudlet(int cloudletId, long cloudletLength, int pesNumber,
                                long fileSize, long outputSize,
                                UtilizationModel utilizationModelCpu,
                                UtilizationModel utilizationModelRam,
                                UtilizationModel utilizationModelBw,
                                double deadline) {
            super(cloudletId, cloudletLength, pesNumber, fileSize, outputSize,
                    utilizationModelCpu, utilizationModelRam, utilizationModelBw);
            this.deadline = deadline;
        }

        public double getDeadline() {
            return deadline;
        }

        public void setDeadline(double deadline) {
            this.deadline = deadline;
        }
    }
}