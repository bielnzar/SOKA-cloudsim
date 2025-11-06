package baru;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.CloudletSchedulerTimeShared;
import org.cloudbus.cloudsim.Datacenter;
import org.cloudbus.cloudsim.DatacenterBroker;
import org.cloudbus.cloudsim.DatacenterCharacteristics;
import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Pe;
import org.cloudbus.cloudsim.Storage;
import org.cloudbus.cloudsim.UtilizationModel;
import org.cloudbus.cloudsim.UtilizationModelFull;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.VmAllocationPolicySimple;
import org.cloudbus.cloudsim.VmSchedulerTimeShared;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.provisioners.BwProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
// ⚠️ sengaja TIDAK meng-import java.io.File supaya tidak bentrok dengan org.cloudbus.cloudsim.File
import java.util.*;
import java.util.stream.Collectors;

/**
 * Baseline Round-Robin sesuai spesifikasi.
 */
public class ContohCloudsim {

    private static final String[] DATASET_FOLDERS = {"randomSimple", "randomStratified", "SDSC"};
    private static final String DATASETS_ROOT = "datasets";
    private static final String OUT_DIR = "out";

    private static final int RUNS_PER_FILE = 10;

    private static final int NUM_DATACENTERS = 6;
    private static final int HOSTS_PER_DC = 3;
    private static final int VMS_PER_HOST = 3;

    // Host spec
    private static final int HOST_RAM_MB = 6144;
    private static final long HOST_STORAGE_MB = 1_000_000L;
    private static final int HOST_BW = 10_000;
    private static final int HOST_PES = 1;
    private static final double HOST_PE_MIPS = 6000.0;
    private static final double HOST_COST_PER_SEC = 3.0;

    // VM spec
    private static final int VM_RAM_MB = 512;
    private static final long VM_SIZE_MB = 10_000L;
    private static final long VM_BW = 1_000;
    private static final int VM_MIPS = 1000;
    private static final int VM_PES = 1;
    private static final String VM_VMM = "Xen";

    // Cloudlet spec
    private static final long CLOUDLET_FILE_SIZE = 300;
    private static final long CLOUDLET_OUTPUT_SIZE = 300;
    private static final int CLOUDLET_PES = 1;

    // ===== SmartBroker (Round-Robin) =====
    public static class SmartBroker extends DatacenterBroker {
        private int rrIndex = 0;
        public SmartBroker(String name) throws Exception { super(name); }
        @Override
        public void submitCloudletList(List<? extends Cloudlet> list) {
            super.submitCloudletList(list);
            if (getVmList() == null || getVmList().isEmpty()) return;
            for (Cloudlet cl : list) {
                int vmId = getVmList().get(rrIndex % getVmList().size()).getId();
                bindCloudletToVm(cl.getCloudletId(), vmId);
                rrIndex++;
            }
        }
    }

    public static void main(String[] args) {
        try {
            ensureDir(OUT_DIR);

            for (String folder : DATASET_FOLDERS) {
                String folderPath = DATASETS_ROOT + java.io.File.separator + folder;
                List<java.io.File> txtFiles = listTxtFiles(folderPath);
                if (txtFiles.isEmpty()) {
                    System.out.println("Tidak ada file .txt di " + folderPath + " — lewati.");
                    continue;
                }

                java.io.File outFile = new java.io.File(OUT_DIR, folder + ".csv");
                java.io.File summaryFile = new java.io.File(OUT_DIR, folder + "_summary.csv");

                try (PrintWriter pw = new PrintWriter(new FileWriter(outFile, false));
                     PrintWriter ps = new PrintWriter(new FileWriter(summaryFile, false))) {

                    pw.println("DatasetFile,Run,TotalCloudlet,TotalCPUTime(s),TotalWaitTime(s),AvgStartTime(s),AvgExecTime(s),AvgFinishTime(s),Throughput(c/s),Makespan(s),ImbalanceDegree,ResourceUtilization(%)");
                    ps.println("DatasetFile,Runs,Avg_Makespan,Std_Makespan,Avg_Throughput,Std_Throughput,Avg_AvgExecTime,Std_AvgExecTime,Avg_Imbalance,Std_Imbalance,Avg_Utilization(%),Std_Utilization(%)");

                    for (java.io.File f : txtFiles) {
                        List<double[]> metricsList = new ArrayList<>();

                        for (int run = 1; run <= RUNS_PER_FILE; run++) {
                            CloudSim.init(1, Calendar.getInstance(), false);

                            for (int dc = 0; dc < NUM_DATACENTERS; dc++) {
                                createDatacenter("DC_" + dc, HOSTS_PER_DC);
                            }

                            SmartBroker broker = new SmartBroker("Broker_" + run);
                            int brokerId = broker.getId();
                            int totalVM = NUM_DATACENTERS * HOSTS_PER_DC * VMS_PER_HOST;
                            List<Vm> vmList = createVmList(brokerId, totalVM);
                            broker.submitVmList(vmList);

                            List<Cloudlet> cloudlets = createCloudletListFromFile(brokerId, f.getPath());
                            if (cloudlets.isEmpty()) break;
                            broker.submitCloudletList(cloudlets);

                            CloudSim.startSimulation();
                            List<Cloudlet> finished = broker.getCloudletReceivedList();
                            CloudSim.stopSimulation();

                            double[] m = computeMetrics(finished, vmList.size());
                            metricsList.add(m);

                            pw.printf(Locale.US,
                                    "%s,%d,%.0f,%.4f,%.4f,%.4f,%.4f,%.4f,%.6f,%.4f,%.6f,%.4f%n",
                                    f.getName(), run,
                                    m[0], m[6], m[1], m[2], m[3], m[4], m[5], m[7], m[8], m[9]);
                        }

                        double avgMakespan = mean(metricsList, 7);
                        double stdMakespan = stddev(metricsList, 7, avgMakespan);
                        double avgThroughput = mean(metricsList, 5);
                        double stdThroughput = stddev(metricsList, 5, avgThroughput);
                        double avgExecTime = mean(metricsList, 3);
                        double stdExecTime = stddev(metricsList, 3, avgExecTime);
                        double avgImb = mean(metricsList, 8);
                        double stdImb = stddev(metricsList, 8, avgImb);
                        double avgUtil = mean(metricsList, 9);
                        double stdUtil = stddev(metricsList, 9, avgUtil);

                        ps.printf(Locale.US,
                                "%s,%d,%.4f,%.4f,%.6f,%.6f,%.4f,%.4f,%.6f,%.6f,%.4f,%.4f%n",
                                f.getName(), RUNS_PER_FILE,
                                avgMakespan, stdMakespan,
                                avgThroughput, stdThroughput,
                                avgExecTime, stdExecTime,
                                avgImb, stdImb,
                                avgUtil, stdUtil);
                    }
                }
            }

            System.out.println("Selesai. CSV ada di folder: " + OUT_DIR);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // -------- Files / dataset helpers --------
    private static List<java.io.File> listTxtFiles(String dirPath) {
        java.io.File d = new java.io.File(dirPath);
        if (!d.exists() || !d.isDirectory()) return Collections.emptyList();
        java.io.File[] arr = d.listFiles((dir, name) -> name.toLowerCase().endsWith(".txt"));
        if (arr == null) return Collections.emptyList();
        return Arrays.stream(arr).sorted(Comparator.comparing(java.io.File::getName)).collect(Collectors.toList());
    }

    private static void ensureDir(String path) {
        java.io.File d = new java.io.File(path);
        if (!d.exists()) d.mkdirs();
    }

    // -------- Infra: datacenter / host / vm --------
    private static Datacenter createDatacenter(String name, int hostsCount) throws Exception {
        List<Host> hostList = new ArrayList<>();
        for (int h = 0; h < hostsCount; h++) {
            List<Pe> peList = new ArrayList<>();
            for (int p = 0; p < HOST_PES; p++) {
                peList.add(new Pe(p, new PeProvisionerSimple(HOST_PE_MIPS)));
            }
            Host host = new Host(
                    h,
                    new RamProvisionerSimple(HOST_RAM_MB),
                    new BwProvisionerSimple(HOST_BW),
                    HOST_STORAGE_MB,
                    peList,
                    new VmSchedulerTimeShared(peList)
            );
            hostList.add(host);
        }

        DatacenterCharacteristics ch = new DatacenterCharacteristics(
                "x86", "Linux", "Xen", hostList, 7.0,
                HOST_COST_PER_SEC, 0.0, 0.0, 0.0
        );

        return new Datacenter(name, ch, new VmAllocationPolicySimple(hostList), new LinkedList<Storage>(), 0);
    }

    private static List<Vm> createVmList(int brokerId, int totalVm) {
        List<Vm> list = new ArrayList<>(totalVm);
        for (int i = 0; i < totalVm; i++) {
            Vm vm = new Vm(i, brokerId, VM_MIPS, VM_PES, VM_RAM_MB, VM_BW, VM_SIZE_MB, VM_VMM, new CloudletSchedulerTimeShared());
            list.add(vm);
        }
        return list;
    }

    // -------- Workload (cloudlets) --------
    private static List<Cloudlet> createCloudletListFromFile(int brokerId, String filePath) {
        List<Long> lengths = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            for (String line; (line = br.readLine()) != null;) {
                line = line.trim();
                if (line.isEmpty()) continue;
                try {
                    long l = Long.parseLong(line);
                    if (l > 0) lengths.add(l);
                } catch (NumberFormatException ignored) {}
            }
        } catch (IOException e) {
            System.out.println("Gagal baca file " + filePath + " -> " + e.getMessage());
        }
        if (lengths.isEmpty()) return Collections.emptyList();

        List<Cloudlet> cloudlets = new ArrayList<>(lengths.size());
        UtilizationModel um = new UtilizationModelFull();
        int id = 0;
        for (long len : lengths) {
            Cloudlet c = new Cloudlet(id++, len, CLOUDLET_PES, CLOUDLET_FILE_SIZE, CLOUDLET_OUTPUT_SIZE, um, um, um);
            c.setUserId(brokerId);
            cloudlets.add(c);
        }
        return cloudlets;
    }

    // -------- Metrics --------
    private static double[] computeMetrics(List<Cloudlet> finished, int numVMs) {
        if (finished == null || finished.isEmpty()) {
            return new double[]{0,0,0,0,0,0,0,0,0,0};
        }

        int n = finished.size();
        double sumWait = 0.0, sumStart = 0.0, sumExec = 0.0, sumFinish = 0.0;
        double minSubmit = Double.POSITIVE_INFINITY, maxFinish = Double.NEGATIVE_INFINITY;

        Map<Integer, Double> execPerVm = new HashMap<>();

        for (Cloudlet c : finished) {
            double submit = c.getSubmissionTime();
            double start = c.getExecStartTime();
            double finish = c.getFinishTime();
            double wait = c.getWaitingTime();
            double exec = c.getActualCPUTime();

            minSubmit = Math.min(minSubmit, submit);
            maxFinish = Math.max(maxFinish, finish);

            sumWait += wait;
            sumStart += start;
            sumExec += exec;
            sumFinish += finish;

            execPerVm.merge(c.getVmId(), exec, Double::sum);
        }

        double makespan = maxFinish - (Double.isFinite(minSubmit) ? minSubmit : 0.0);
        if (makespan <= 0) makespan = maxFinish;

        double avgStart = sumStart / n;
        double avgExec = sumExec / n;
        double avgFinish = sumFinish / n;
        double throughput = (makespan > 0) ? (n / makespan) : 0.0;
        double totalCpuTime = sumExec;

        double minExecVM = Double.POSITIVE_INFINITY, maxExecVM = Double.NEGATIVE_INFINITY, sumExecVM = 0.0;
        for (double v : execPerVm.values()) {
            minExecVM = Math.min(minExecVM, v);
            maxExecVM = Math.max(maxExecVM, v);
            sumExecVM += v;
        }
        double avgExecPerVm = execPerVm.isEmpty() ? 0.0 : (sumExecVM / execPerVm.size());
        double imbalance = (avgExecPerVm > 0) ? (maxExecVM - minExecVM) / avgExecPerVm : 0.0;

        double utilizationPct = (makespan > 0 && numVMs > 0) ? (totalCpuTime / (numVMs * makespan)) * 100.0 : 0.0;

        return new double[]{ n, sumWait, avgStart, avgExec, avgFinish, throughput, totalCpuTime, makespan, imbalance, utilizationPct };
    }

    private static double mean(List<double[]> list, int idx) {
        if (list == null || list.isEmpty()) return 0.0;
        double s = 0.0;
        for (double[] a : list) s += a[idx];
        return s / list.size();
    }

    private static double stddev(List<double[]> list, int idx, double mean) {
        if (list == null || list.size() <= 1) return 0.0;
        double s = 0.0;
        for (double[] a : list) {
            double d = a[idx] - mean;
            s += d * d;
        }
        return Math.sqrt(s / (list.size() - 1));
    }
}
