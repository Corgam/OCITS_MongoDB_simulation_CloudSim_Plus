import ch.qos.logback.classic.Level;
import org.cloudbus.cloudsim.allocationpolicies.VmAllocationPolicyBestFit;
import org.cloudbus.cloudsim.brokers.DatacenterBroker;
import org.cloudbus.cloudsim.brokers.DatacenterBrokerBestFit;
import org.cloudbus.cloudsim.cloudlets.Cloudlet;
import org.cloudbus.cloudsim.cloudlets.CloudletSimple;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.datacenters.Datacenter;
import org.cloudbus.cloudsim.datacenters.DatacenterSimple;
import org.cloudbus.cloudsim.hosts.Host;
import org.cloudbus.cloudsim.hosts.HostSimple;
import org.cloudbus.cloudsim.resources.Pe;
import org.cloudbus.cloudsim.resources.PeSimple;
import org.cloudbus.cloudsim.utilizationmodels.UtilizationModelDynamic;
import org.cloudbus.cloudsim.vms.Vm;
import org.cloudbus.cloudsim.vms.VmSimple;
import org.cloudsimplus.builders.tables.CloudletsTableBuilder;
import org.cloudsimplus.listeners.VmHostEventInfo;
import org.cloudsimplus.util.Log;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * An example with just single Host and VM, and many Cloudlets
 * in order to simulate a waiting queue situation for the Cloudlets.
 *
 * With the limited resources of the single VM, Cloudlets are waiting for their turn.
 * By using the Event Listener, after the status change of the VM, the broker submits the
 * rest of the unfinished Cloudlets until all are successfully completed.
 *
 * @author Emil Balitzki
 * @since ClodSimPlus ---
 */
public class MongoDBOneShardScenario {
    private static final int HOSTS = 1;
    private static final int HOST_PES = 4;

    private static final int VMS = 1;
    private static final int VM_PES = 4;

    private static final int CLOUDLETS = 50;
    private static final int CLOUDLET_PES = 4;
    private static final int CLOUDLET_LENGTH = 10000;

    private final CloudSim simulation;
    private final DatacenterBroker broker0;
    private final List<Cloudlet> cloudletList;

    public static void main(String[] args) {
        new MongoDBOneShardScenario();
    }

    private MongoDBOneShardScenario() {
        simulation = new CloudSim();
        Datacenter datacenter0 = createDatacenter();

        // Creates a broker that is a software acting on behalf a cloud customer to manage his/her VMs and Cloudlets
        // The best fit broker is used to not overwhelm the VMs.
        broker0 = new DatacenterBrokerBestFit(simulation);
        Log.setLevel(DatacenterBroker.LOGGER, Level.OFF);

        List<Vm> vmList = createVms();
        cloudletList = createCloudlets();
        broker0.submitVmList(vmList);
        broker0.submitCloudletList(cloudletList);

        // Append an event listener to all VMs, which is called every status change of the given VM.
        for (Vm singleVm : vmList){
            singleVm.addOnUpdateProcessingListener(this::vmProcessingUpdateListener);
        }
        // Start the simulation
        simulation.start();
        // Get the results
        final List<Cloudlet> finishedCloudlets = broker0.getCloudletFinishedList();
        finishedCloudlets.sort(Comparator.comparingLong(cloudlet -> cloudlet.getVm().getId()));
        new CloudletsTableBuilder(finishedCloudlets).build();
    }

    /**
     * VM's update listener function.
     * Whenever VM changes it status, try to submit new, unfinished Cloudlets.
     */
    private void vmProcessingUpdateListener(VmHostEventInfo info) {
        broker0.submitCloudletList(cloudletList);
    }

    /**
     * Creates a Datacenter and its Hosts.
     */
    private Datacenter createDatacenter() {
        final List<Host> hostList = new ArrayList<>(HOSTS);
        for(int i = 0; i < HOSTS; i++) {
            Host host = createHost();
            hostList.add(host);
        }

        return new DatacenterSimple(simulation, hostList, new VmAllocationPolicyBestFit());
    }

    private Host createHost() {
        final List<Pe> peList = new ArrayList<>(HOST_PES);
        //List of Host's CPUs (Processing Elements, PEs)
        for (int i = 0; i < HOST_PES; i++) {
            //Uses a PeProvisionerSimple by default to provision PEs for VMs
            peList.add(new PeSimple(1000));
        }

        final long ram = 16384; //in Megabytes
        final long bw = 10000; //in Megabits/s
        final long storage = 1000000; //in Megabytes

        /*
        Uses ResourceProvisionerSimple by default for RAM and BW provisioning
        and VmSchedulerSpaceShared for VM scheduling.
        */
        return new HostSimple(ram, bw, storage, peList, false);
    }

    /**
     * Creates a list of VMs.
     */
    private List<Vm> createVms() {
        final List<Vm> list = new ArrayList<>(VMS);
        for (int i = 0; i < VMS; i++) {
            final Vm vm = new VmSimple(1000, VM_PES);
            vm.setRam(16384).setBw(1000).setSize(10000);
            // Wait until resources are available.
            //vm.setCloudletScheduler(new CloudletSchedulerSpaceShared());
            list.add(vm);
        }

        return list;
    }

    /**
     * Creates a list of Cloudlets.
     */
    private List<Cloudlet> createCloudlets() {
        final List<Cloudlet> list = new ArrayList<>(CLOUDLETS);

        final UtilizationModelDynamic utilizationModel = new UtilizationModelDynamic(0.8);

        for (int i = 0; i < CLOUDLETS; i++) {
            final Cloudlet cloudlet = new CloudletSimple(CLOUDLET_LENGTH, CLOUDLET_PES, utilizationModel);
            cloudlet.setSizes(1024);
            list.add(cloudlet);
        }

        return list;
    }
}
