public class Mandelbrot {

    static final int THREADS = 12;         // число потоков
    static final int WIDTH = 1200;         // ширина картинки в пикселях
    static final int HEIGHT = 800;         // высота картинки в пикселях
    static final int MAX_ITER = 1000;      // максимум итераций на один пиксель

    // Область комплексной плоскости
    static final double X_MIN = -2.0;
    static final double X_MAX = 1.0;
    static final double Y_MIN = -1.0;
    static final double Y_MAX = 1.0;

    // Шаг по плоскости, соответствующий одному пикселю
    static final double dx = (X_MAX - X_MIN) / WIDTH;
    static final double dy = (Y_MAX - Y_MIN) / HEIGHT;

    // Для точки c = cr + ci*i считаем z = z^2 + c, начиная с z = 0.
    // Если |z| > 2, точка точно не входит в множество (она "убежала").
    // Если за MAX_ITER итераций не убежала - считаем, что точка входит в множество.
    static boolean inSet(double cr, double ci) {
        double zr = 0.0;
        double zi = 0.0;
        for (int i = 0; i < MAX_ITER; i++) {
            if (zr * zr + zi * zi > 4.0) {
                return false;
            }
            double newZr = zr * zr - zi * zi + cr;   // действительная часть z^2 + c
            zi = 2 * zr * zi + ci;                   // мнимая часть z^2 + c
            zr = newZr;
        }
        return true;
    }

    // Считаем одну строку пикселей: true - пиксель входит в множество
    static void computeRow(int y, boolean[] pixels) {
        double ci = Y_MIN + dy * y;
        for (int x = 0; x < WIDTH; x++) {
            double cr = X_MIN + dx * x;
            pixels[y * WIDTH + x] = inSet(cr, ci);
        }
    }

    public static void main(String[] args) throws InterruptedException {
        sequential();
        parallel();
    }

    // 1. Последовательный способ
    static void sequential() {
        boolean[] pixels = new boolean[WIDTH * HEIGHT];

        long start = System.nanoTime();

        for (int y = 0; y < HEIGHT; y++) {
            computeRow(y, pixels);
        }

        long end = System.nanoTime();

        System.out.println("Последовательный способ, время (мс): " + (end - start) / 1000000.0);
    }

    // 2. Параллельный способ
    static void parallel() throws InterruptedException {
        // Общий массив. Каждый поток пишет только в свои строки,
        // поэтому синхронизация не нужна.
        boolean[] pixels = new boolean[WIDTH * HEIGHT];
        Thread[] threads = new Thread[THREADS];

        for (int t = 0; t < THREADS; t++) {
            threads[t] = createThread(t, pixels);
        }

        long start = System.nanoTime();

        for (int t = 0; t < THREADS; t++) {
            threads[t].start();
        }
        for (int t = 0; t < THREADS; t++) {
            threads[t].join();
        }

        long end = System.nanoTime();

        System.out.println("Параллельный способ, время (мс): " + (end - start) / 1000000.0);
    }

    // Поток t считает строки t, t + THREADS, t + 2*THREADS, ...
    // Строки чередуются специально: в центре (где само множество) считать
    // намного дольше, чем по краям, и так нагрузка между потоками делится поровну.
    static Thread createThread(int id, boolean[] pixels) {
        return new Thread(() -> {
            for (int y = id; y < HEIGHT; y += THREADS) {
                computeRow(y, pixels);
            }
        });
    }
}