package perbandingan;

import org.cloudbus.cloudsim.*;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.provisioners.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class pso {

    // Infrastruktur
    private static final int NUM_DATACENTERS = 6;
    private static final int HOSTS_PER_DC = 3;
    private static final int VMS_PER_HOST = 3;

    // Host
    private static final int HOST_RAM_MB = 6144;
    private static final long HOST_STORAGE_MB = 1_000_000L;
    private static final int HOST_BW = 10_000;
    private static final int HOST_PES = 1;
    private static final int HOST_PE_MIPS = 6000;
    private static final double COST_PER_SEC = 3.0;
    private static final double HOST_POWER = 200.0;

    // VM
    private static final int VM_RAM_MB = 512;
    private static final long VM_STORAGE_MB = 10_000;
    private static final long VM_BW = 1_000;
    private static final int VM_MIPS_BASE = 1000;
    private static final int VM_PES = 1;

    // Cloudlet
    private static final long CLOUDLET_FILE_SIZE = 300;
    private static final long CLOUDLET_OUTPUT_SIZE = 300;
    private static final int CLOUDLET_PES = 1;

    // Output
    private static final String OUTPUT_DIR = "hasil";
    private static final String CSV_HEADER =
            "Dataset,Trial,TotalCPUTime,TotalWaitTime,AverageStartTime,AverageExecTime,AverageFinishTime,Throughput,Makespan,ImbalanceDegree,ResourceUtilization,TotalEnergyConsumption";

    // Variasi antar percobaan
    private static final boolean VARIASI_VM_MIPS = true;

    // Parameter PSO
    private static final int PSO_PARTICLES = 30;
    private static final int PSO_ITERATIONS = 100;
    private static final double PSO_KEEP_PROB = 0.4;
    private static final double PSO_PBEST_PROB = 0.3;
    private static final double PSO_GBEST_PROB = 0.3;
    private static final double PSO_MUTATION_PROB = 0.02;

    public static void main(String[] args) {
        System.out.println("CloudSim Simulation - PSO-based Cloudlet to VM Mapping (TimeShared)");
        System.out.println("Contoh path: datasets/randomSimple/RandSimple3000.txt");

        try (Scanner sc = new Scanner(System.in)) {
            System.out.print("Masukkan path file dataset: ");
            String datasetPath = sc.nextLine().trim();
            if (datasetPath.isEmpty()) {
                System.out.println("Path kosong. Program dihentikan.");
                return;
            }

            System.out.print("Masukkan nama/label dataset: ");
            String datasetLabel = sc.nextLine().trim();
            if (datasetLabel.isEmpty()) {
                datasetLabel = deriveDefaultLabel(datasetPath);
            }

            final int TRIALS = 10;

            ensureOutputDir();
            String csvPath = OUTPUT_DIR + "/" + getFolderTag(datasetPath) + "_" + safeCsvName(datasetLabel) + "_PSO.csv";
            initCsv(csvPath);

            List<double[]> allTrials = new ArrayList<>();
            for (int t = 1; t <= TRIALS; t++) {
                Metrics m = runSingleSimulation(datasetPath, datasetLabel, t);
                allTrials.add(m.toArray());

                System.out.printf(Locale.US,
                        "Percobaan %d | TotalCPU=%.2f, TotalWait=%.2f, AvgStart=%.2f, AvgExec=%.2f, AvgFinish=%.2f, Thpt=%.4f, Makespan=%.2f, Imbalance=%.4f, Util=%.4f, Energy=%.2f%n",
                        t, m.totalCpuTime, m.totalWaitTime, m.avgStartTime, m.avgExecTime, m.avgFinishTime,
                        m.throughput, m.makespan, m.imbalanceDegree, m.resourceUtilization, m.totalEnergy);

                appendCsv(csvPath, String.format(Locale.US,
                        "%s,%d,%.4f,%.4f,%.4f,%.4f,%.4f,%.6f,%.4f,%.6f,%.6f,%.4f",
                        datasetLabel, t, m.totalCpuTime, m.totalWaitTime, m.avgStartTime, m.avgExecTime,
                        m.avgFinishTime, m.throughput, m.makespan, m.imbalanceDegree, m.resourceUtilization, m.totalEnergy));
            }

            Metrics avg = Metrics.average(allTrials);
            System.out.println("\nRata-rata dari 10 percobaan:");
            System.out.printf(Locale.US,
                    "TotalCPU=%.2f, TotalWait=%.2f, AvgStart=%.2f, AvgExec=%.2f, AvgFinish=%.2f, Thpt=%.4f, Makespan=%.2f, Imbalance=%.4f, Util=%.4f, Energy=%.2f%n",
                    avg.totalCpuTime, avg.totalWaitTime, avg.avgStartTime, avg.avgExecTime, avg.avgFinishTime,
                    avg.throughput, avg.makespan, avg.imbalanceDegree, avg.resourceUtilization, avg.totalEnergy);

            System.out.println("CSV hasil disimpan di: " + csvPath);

        } catch (Exception e) {
            System.out.println("Terjadi kesalahan:");
            e.printStackTrace();
        }
    }

    private static Metrics runSingleSimulation(String datasetPath, String datasetLabel, int trial) throws Exception {
        long[] dataset = loadDataset(datasetPath);

        // Variasi agar tiap percobaan berbeda
        shuffleArray(dataset, new Random(12345L + trial));

        CloudSim.init(1, Calendar.getInstance(), false);

        DatacenterBroker broker = new DatacenterBroker("Broker");
        int brokerId = broker.getId();

        List<Datacenter> dcs = new ArrayList<>();
        for (int i = 0; i < NUM_DATACENTERS; i++) {
            dcs.add(createDatacenter("DC_" + i).datacenter);
        }

        int totalVm = NUM_DATACENTERS * HOSTS_PER_DC * VMS_PER_HOST; // 54
        Random vmRnd = new Random(999L + trial);
        List<Vm> vmList = createVmList(brokerId, totalVm, vmRnd);
        broker.submitVmList(vmList);

        // Jalankan PSO untuk menentukan pemetaan cloudlet -> VM
        int[] mapping = psoAssign(dataset, vmList, new Random(2025L + trial));

        // Buat cloudlet dan set VM sesuai hasil PSO
        List<Cloudlet> cloudlets = createCloudletsWithMapping(brokerId, vmList, dataset, mapping);
        broker.submitCloudletList(cloudlets);

        CloudSim.startSimulation();
        List<Cloudlet> finished = broker.getCloudletReceivedList();
        CloudSim.stopSimulation();

        return computeMetrics(finished, vmList, dataset.length);
    }

    // ===== PSO Discrete untuk minimisasi makespan estimasi =====
    private static int[] psoAssign(long[] lengths, List<Vm> vmList, Random rnd) {
        int n = lengths.length;
        int m = vmList.size();

        // kapasitas VM (MI per detik)
        double[] vmMips = new double[m];
        for (int j = 0; j < m; j++) vmMips[j] = vmList.get(j).getMips();

        int[][] pos = new int[PSO_PARTICLES][n];
        int[][] pbestPos = new int[PSO_PARTICLES][n];
        double[] pbestVal = new double[PSO_PARTICLES];

        // inisialisasi partikel
        for (int p = 0; p < PSO_PARTICLES; p++) {
            for (int i = 0; i < n; i++) pos[p][i] = rnd.nextInt(m);
            pbestPos[p] = pos[p].clone();
            pbestVal[p] = evalMakespan(lengths, vmMips, pos[p]);
        }

        // global best
        int gIdx = argmin(pbestVal);
        int[] gbestPos = pbestPos[gIdx].clone();
        double gbestVal = pbestVal[gIdx];

        for (int it = 0; it < PSO_ITERATIONS; it++) {
            for (int p = 0; p < PSO_PARTICLES; p++) {
                // update diskret per cloudlet
                for (int i = 0; i < n; i++) {
                    double r = rnd.nextDouble();
                    if (r < PSO_KEEP_PROB) {
                        // pertahankan
                    } else if (r < PSO_KEEP_PROB + PSO_PBEST_PROB) {
                        pos[p][i] = pbestPos[p][i];
                    } else if (r < PSO_KEEP_PROB + PSO_PBEST_PROB + PSO_GBEST_PROB) {
                        pos[p][i] = gbestPos[i];
                    } else {
                        pos[p][i] = rnd.nextInt(m);
                    }
                    // mutasi kecil
                    if (rnd.nextDouble() < PSO_MUTATION_PROB) {
                        pos[p][i] = rnd.nextInt(m);
                    }
                }

                double val = evalMakespan(lengths, vmMips, pos[p]);
                if (val < pbestVal[p]) {
                    pbestVal[p] = val;
                    pbestPos[p] = pos[p].clone();
                    if (val < gbestVal) {
                        gbestVal = val;
                        gbestPos = pbestPos[p].clone();
                    }
                }
            }
        }
        return gbestPos;
    }

    private static double evalMakespan(long[] lengths, double[] vmMips, int[] assign) {
        int m = vmMips.length;
        double[] load = new double[m]; // total waktu kerja estimasi per VM (detik)
        for (int i = 0; i < lengths.length; i++) {
            int v = assign[i];
            double t = lengths[i] / vmMips[v]; // detik
            load[v] += t;
        }
        double max = 0;
        for (double x : load) if (x > max) max = x;
        return max;
    }

    private static int argmin(double[] a) {
        int idx = 0;
        double best = a[0];
        for (int i = 1; i < a.length; i++) if (a[i] < best) { best = a[i]; idx = i; }
        return idx;
    }

    // ===== CloudSim helpers =====
    private static long[] loadDataset(String filePath) throws IOException {
        List<Long> vals = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String s;
            while ((s = br.readLine()) != null) {
                s = s.trim();
                if (s.isEmpty()) continue;
                try { vals.add(Long.parseLong(s)); } catch (NumberFormatException ignored) {}
            }
        }
        long[] arr = new long[vals.size()];
        for (int i = 0; i < vals.size(); i++) arr[i] = vals.get(i);
        return arr;
    }

    private static void shuffleArray(long[] a, Random rnd) {
        for (int i = a.length - 1; i > 0; i--) {
            int j = rnd.nextInt(i + 1);
            long tmp = a[i]; a[i] = a[j]; a[j] = tmp;
        }
    }

    private static class DatacenterInfo {
        Datacenter datacenter;
        DatacenterCharacteristics characteristics;
        DatacenterInfo(Datacenter d, DatacenterCharacteristics c) { datacenter = d; characteristics = c; }
    }

    private static DatacenterInfo createDatacenter(String name) throws Exception {
        List<Host> hostList = new ArrayList<>();
        for (int h = 0; h < HOSTS_PER_DC; h++) {
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
                "x86", "Linux", "Xen",
                hostList, 7.0,
                COST_PER_SEC, 0.0, 0.0, 0.0
        );

        Datacenter dc = new Datacenter(
                name, ch,
                new VmAllocationPolicySimple(hostList),
                new LinkedList<Storage>(),
                0
        );
        return new DatacenterInfo(dc, ch);
    }

    private static List<Vm> createVmList(int brokerId, int totalVm, Random rnd) {
        List<Vm> list = new ArrayList<>(totalVm);
        for (int i = 0; i < totalVm; i++) {
            int mips = VM_MIPS_BASE;
            if (VARIASI_VM_MIPS) {
                mips = (int) Math.round(VM_MIPS_BASE * (0.9 + rnd.nextDouble() * 0.2));
            }
            Vm vm = new Vm(
                    i, brokerId, mips, VM_PES,
                    VM_RAM_MB, VM_BW, VM_STORAGE_MB, "Xen",
                    new CloudletSchedulerTimeShared()
            );
            list.add(vm);
        }
        return list;
    }

    private static List<Cloudlet> createCloudletsWithMapping(int brokerId, List<Vm> vmList, long[] lengths, int[] mapping) {
        List<Cloudlet> list = new ArrayList<>(lengths.length);
        UtilizationModel util = new UtilizationModelFull();
        for (int i = 0; i < lengths.length; i++) {
            Cloudlet cl = new Cloudlet(i, lengths[i], CLOUDLET_PES, CLOUDLET_FILE_SIZE, CLOUDLET_OUTPUT_SIZE, util, util, util);
            cl.setUserId(brokerId);
            int vmId = vmList.get(mapping[i]).getId();
            cl.setVmId(vmId);
            list.add(cl);
        }
        return list;
    }

    // ===== Metrik & CSV =====
    private static class Metrics {
        double totalCpuTime;
        double totalWaitTime;
        double avgStartTime;
        double avgExecTime;
        double avgFinishTime;
        double throughput;
        double makespan;
        double imbalanceDegree;
        double resourceUtilization;
        double totalEnergy;

        double[] toArray() {
            return new double[]{
                    totalCpuTime, totalWaitTime, avgStartTime, avgExecTime,
                    avgFinishTime, throughput, makespan, imbalanceDegree,
                    resourceUtilization, totalEnergy
            };
        }

        static Metrics average(List<double[]> rows) {
            Metrics m = new Metrics();
            int n = rows.size();
            for (double[] r : rows) {
                m.totalCpuTime += r[0];
                m.totalWaitTime += r[1];
                m.avgStartTime += r[2];
                m.avgExecTime += r[3];
                m.avgFinishTime += r[4];
                m.throughput += r[5];
                m.makespan += r[6];
                m.imbalanceDegree += r[7];
                m.resourceUtilization += r[8];
                m.totalEnergy += r[9];
            }
            m.totalCpuTime /= n;
            m.totalWaitTime /= n;
            m.avgStartTime /= n;
            m.avgExecTime /= n;
            m.avgFinishTime /= n;
            m.throughput /= n;
            m.makespan /= n;
            m.imbalanceDegree /= n;
            m.resourceUtilization /= n;
            m.totalEnergy /= n;
            return m;
        }
    }

    private static Metrics computeMetrics(List<Cloudlet> finished, List<Vm> vmList, int totalCloudlets) {
        Metrics m = new Metrics();
        if (finished == null || finished.isEmpty()) return m;

        double sumStart = 0, sumExec = 0, sumFinish = 0;
        double minStart = Double.POSITIVE_INFINITY, maxFinish = 0;
        double totalCpu = 0, totalWait = 0;

        Map<Integer, Double> workPerVm = new HashMap<>();
        for (Vm vm : vmList) workPerVm.put(vm.getId(), 0.0);

        for (Cloudlet c : finished) {
            double start = c.getExecStartTime();
            double finish = c.getFinishTime();
            double cpu = c.getActualCPUTime();
            double wait = c.getWaitingTime();

            sumStart += start;
            sumExec += cpu;
            sumFinish += finish;
            if (start < minStart) minStart = start;
            if (finish > maxFinish) maxFinish = finish;

            totalCpu += cpu;
            totalWait += wait;

            workPerVm.put(c.getVmId(), workPerVm.getOrDefault(c.getVmId(), 0.0) + c.getCloudletLength());
        }

        int n = finished.size();
        m.totalCpuTime = totalCpu;
        m.totalWaitTime = totalWait;
        m.avgStartTime = sumStart / n;
        m.avgExecTime = sumExec / n;
        m.avgFinishTime = sumFinish / n;

        m.makespan = maxFinish;
        double busyWindow = Math.max(1e-9, maxFinish - Math.max(0, minStart));
        m.throughput = n / busyWindow;

        double maxLoad = 0, minLoad = Double.POSITIVE_INFINITY;
        for (double mi : workPerVm.values()) {
            maxLoad = Math.max(maxLoad, mi);
            minLoad = Math.min(minLoad, mi);
        }
        m.imbalanceDegree = (maxLoad == 0) ? 0 : (maxLoad - minLoad) / maxLoad;

        double totalVmCapacityPerSec = 0;
        for (Vm vm : vmList) totalVmCapacityPerSec += vm.getMips();
        double capacityOverWindow = totalVmCapacityPerSec * busyWindow;
        double totalMiExecuted = 0;
        for (Cloudlet c : finished) totalMiExecuted += c.getCloudletLength();
        m.resourceUtilization = (capacityOverWindow == 0) ? 0 : (totalMiExecuted / capacityOverWindow);

        int activeHosts = NUM_DATACENTERS * HOSTS_PER_DC;
        m.totalEnergy = activeHosts * HOST_POWER * m.makespan;

        return m;
    }

    private static void ensureOutputDir() throws IOException {
        Path p = Paths.get(OUTPUT_DIR);
        if (!Files.exists(p)) Files.createDirectories(p);
    }

    private static String safeCsvName(String datasetPath) {
        return datasetPath.replaceAll("[^a-zA-Z0-9-_\\.]", "_");
    }

    private static void initCsv(String csvPath) throws IOException {
        java.io.File f = new java.io.File(csvPath);
        if (!f.exists()) {
            try (PrintWriter pw = new PrintWriter(new FileWriter(f, false))) {
                pw.println(CSV_HEADER);
            }
        }
    }

    private static void appendCsv(String csvPath, String line) throws IOException {
        try (PrintWriter pw = new PrintWriter(new FileWriter(csvPath, true))) {
            pw.println(line);
        }
    }

    private static String getFolderTag(String datasetPath) {
        String p = datasetPath.replace('\\', '/');
        String[] parts = p.split("/");
        if (parts.length >= 2) return parts[parts.length - 2];
        return "dataset";
    }

    private static String deriveDefaultLabel(String datasetPath) {
        String p = datasetPath.replace('\\', '/');
        String file = p.substring(p.lastIndexOf('/') + 1);
        int dot = file.lastIndexOf('.');
        return (dot > 0) ? file.substring(0, dot) : file;
    }
}
