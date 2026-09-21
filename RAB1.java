public class RAB1 {

    static final int THREADS = 16;         // число потоков
    static final int n = 100000000;        // число прямоугольников
    static final int N_PER_THREAD = n / THREADS;

    static final double l = -3.0;          // левая граница
    static final double r = 67.0;          // правая граница
    static final double d = (r - l) / n;   // ширина прямоугольника

    // Подынтегральная функция: f(x) = x*sin(x) + x^2 * e^(-x/2) + 1/(1+x)
    static double f(double x) {
        return x * Math.sin(x) + x * x * Math.exp(-x / 2) + 1 / (1 + x);
    }

    // Первообразная: F(x) = sin(x) - x*cos(x) - 2*e^(-x/2)*(x^2 + 4x + 8) + ln(1+x)
    static double F(double x) {
        return Math.sin(x) - x * Math.cos(x)
                - 2 * Math.exp(-x / 2) * (x * x + 4 * x + 8)
                + Math.log(1 + x);
    }

    // Точное значение интеграла
    static double exact() {
        return F(r) - F(l);
    }

    public static void main(String[] args) throws InterruptedException {
        System.out.println("d = " + d);

        sequential();
        System.out.println();
        parallel();
    }

    // 1. Последовательный способ
    static void sequential() {
        long start = System.nanoTime();

        double sum = 0.0;
        for (int i = 0; i < n; i++) {
            double x = l + d * i;
            sum += f(x) * d;
        }

        long end = System.nanoTime();

        System.out.println("Последовательный способ");
        System.out.println("Результат: " + sum);
        System.out.println("Точное значение: " + exact());
        System.out.println("Время (мс): " + (end - start) / 1000000.0);
    }

    // 2. Параллельный способ
    static void parallel() throws InterruptedException {
        double[] results = new double[THREADS];   // у каждого потока своя ячейка
        Thread[] threads = new Thread[THREADS];

        for (int t = 0; t < THREADS; t++) {
            threads[t] = createThread(t, results);
        }

        long start = System.nanoTime();

        for (int t = 0; t < THREADS; t++) {
            threads[t].start();
        }
        for (int t = 0; t < THREADS; t++) {
            threads[t].join();
        }

        // Складываем частичные суммы
        double sum = 0.0;
        for (int t = 0; t < THREADS; t++) {
            sum += results[t];
        }

        long end = System.nanoTime();

        System.out.println("Параллельный способ");
        System.out.println("Результат: " + sum);
        System.out.println("Точное значение: " + exact());
        System.out.println("Время (мс): " + (end - start) / 1000000.0);
    }

    // Поток считает свою часть интервала
    static Thread createThread(int id, double[] results) {
        return new Thread(() -> {
            int from = id * N_PER_THREAD;
            int to = from + N_PER_THREAD;

            double sum = 0.0;
            for (int i = from; i < to; i++) {
                double x = l + d * i;
                sum += f(x) * d;
            }

            results[id] = sum;
        });
    }
}
