import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/*
 * ПРАКТИЧЕСКОЕ ЗАДАНИЕ: МНОГОПОТОЧНЫЙ АНАЛИЗ ЛОГОВ ВЕБ-СЕРВЕРА
 *
 * Крупные части задания:
 * 1. Сгенерировать набор настоящих файлов с логами.
 * 2. Реализовать разбор строк и последовательный отчёт.
 * 3. Распараллелить обработку нескольких файлов через ExecutorService.
 * 4. Сделать общую статистику потокобезопасной.
 * 5. Ограничить число одновременно читаемых файлов через Semaphore.
 * 6. Добавить Atomic-счётчики, CAS и корректное ожидание завершения.
 * 7. Реализовать досрочную остановку анализа через volatile.
 * 8. Сформулировать и доказать инварианты корректности и отсутствия дедлоков.
 * 9. Реализовать обработку повреждённых файлов и частичный отчёт.
 *
 * ФАЙЛ МОЖНО ЗАПУСТИТЬ САМОСТОЯТЕЛЬНО:
 *   java LogAnalysisAssignment generate logs 20 1000
 *   java LogAnalysisAssignment check
 *
 * Первая команда создаёт каталог logs и файлы access-000.log, access-001.log и т.д.
 * Аргументы: каталог, количество файлов, количество строк в каждом файле.
 * Вторая команда выполняет встроенную проверку генератора и итератора строк.
 *
 * ФОРМАТ СТРОКИ:
 *   METHOD PATH STATUS RESPONSE_BYTES DURATION_MILLIS
 * Например:
 *   GET /api/users 200 820 4
 *
 * Поля разделены одним или несколькими пробелами.
 * Успешный запрос имеет статус от 200 до 399 включительно.
 * Ошибочный запрос имеет статус от 400 и выше.
 *
 * ГОТОВЫЕ СТРУКТУРЫ:
 *   LogEntry       — одна разобранная запись.
 *   LogFile        — путь к одному файлу.
 *   PathStatistics — неизменяемая статистика одного URL.
 *   Report         — итоговый отчёт.
 *   LogFileLines   — итератор строк файла.
 *   GenerateLogs   — генератор входных файлов.
 *
 * ВАЖНО ПРО LogFileLines:
 * Итератор потоконебезопасен. Один экземпляр Iterator<String> нельзя читать
 * из нескольких потоков одновременно. Каждый поток должен получить собственный
 * итератор для своего файла. Нельзя разделить один итератор между задачами.
 * Сам файл можно читать параллельно разными потоками только при условии, что
 * каждый поток создаёт отдельный итератор.
 *
 * ЧАСТИ ЗАДАНИЯ:
 *
 * 1. РАЗБОР СТРОКИ
 * Реализуйте преобразование одной строки в LogEntry. Проверьте количество
 * полей, преобразуйте числовые значения и выбрасывайте понятную ошибку для
 * повреждённой строки.
 *
 * 2. ПОСЛЕДОВАТЕЛЬНЫЙ ОТЧЁТ
 * Напишите обработку всех файлов одним потоком. Для каждого URL собирайте
 * количество запросов, количество успешных запросов и ошибок, объём ответов,
 * суммарное и максимальное время обработки. Этот вариант станет эталоном для
 * проверки многопоточной версии.
 *
 * 3. ПАРАЛЛЕЛЬНАЯ ОБРАБОТКА ФАЙЛОВ
 * Создайте фиксированный ExecutorService. Каждый файл обрабатывается отдельной
 * задачей. Рабочий поток создаёт собственный LogFileLines.Iterator, читает свой
 * файл и передаёт записи в общий аккумулятор. Главный поток должен дождаться
 * завершения всех файлов и только потом построить Report.
 *
 * 4. ПОТОКОБЕЗОПАСНАЯ СТАТИСТИКА
 * Реализуйте аккумулятор статистики. Сначала допустимо использовать один общий
 * synchronized-метод. Затем реализуйте вариант с отдельным synchronized-аккумулятором
 * для каждого URL и ConcurrentHashMap. Сравните эти варианты на данных, где почти
 * все запросы относятся к одному URL, и на данных с большим числом URL.
 *
 * 5. ОГРАНИЧЕНИЕ ЧТЕНИЯ ФАЙЛОВ
 * Добавьте Semaphore, ограничивающий число файлов, которые могут обрабатываться
 * одновременно. Рабочих потоков может быть больше разрешений семафора. Разрешение
 * нужно получить перед чтением файла и гарантированно вернуть после завершения.
 * Добавьте наблюдение за текущим и максимальным числом активных файлов.
 *
 * 6. ATOMIC-СЧЁТЧИКИ И CAS
 * Добавьте счётчики обработанных файлов, прочитанных строк, корректных записей,
 * повреждённых записей, ошибок чтения и активных файлов. Для максимального числа
 * активных файлов используйте AtomicInteger и compareAndSet. Обычные изменяемые
 * int/long для этих глобальных счётчиков не используйте.
 *
 * 7. ОЖИДАНИЕ ЗАВЕРШЕНИЯ
 * Организуйте ожидание всех файлов через CountDownLatch либо через набор Future.
 * Завершение задачи должно учитываться даже при исключении. ExecutorService нужно
 * корректно закрыть и дождаться его завершения.
 *
 * CountDownLatch — это одноразовый счётчик-сигнал. Его создают с начальным
 * числом, например равным количеству файлов. Главный поток вызывает await()
 * и блокируется, пока счётчик не станет равен нулю. Каждая завершившаяся задача
 * один раз вызывает countDown(), уменьшая счётчик. Когда последний счётчик
 * уменьшен до нуля, все ожидающие потоки продолжают работу. Увеличить latch
 * обратно или использовать его повторно нельзя, поэтому для нового запуска
 * создаётся новый объект. Вызов countDown() нужно помещать в finally, чтобы
 * исключение в задаче не оставило главный поток ждать бесконечно.
 *
 * 8. ДОСРОЧНАЯ ОСТАНОВКА
 * Добавьте volatile-флаг остановки. После остановки новые строки не начинают
 * обрабатываться, а уже начатая операция может завершиться. Опишите, какие данные
 * входят в частичный отчёт. Не оставляйте рабочие потоки и разрешения семафора
 * в подвешенном состоянии.
 *
 * 9. ПОВРЕЖДЁННЫЕ ФАЙЛЫ И ЧАСТИЧНЫЙ ОТЧЁТ
 * Сделайте так, чтобы ошибка одного файла не отменяла обработку остальных файлов.
 * В итоговом результате отдельно храните список или число файлов, которые не удалось
 * обработать. Решите, что происходит при повреждённой строке: прекращается обработка
 * всего файла или пропускается только эта строка. Выберите один вариант и явно
 * зафиксируйте его в документации. Для полного запуска корректные файлы должны
 * давать полный отчёт, а ошибка должна быть видна пользователю.
 *
 * ИНВАРИАНТЫ, КОТОРЫЕ НУЖНО ДОКАЗАТЬ:
 * - каждая строка полного запуска обработана ровно один раз;
 * - для каждого URL requests = successful + errors;
 * - maxDuration <= totalDuration при наличии запросов;
 * - 0 <= activeFiles <= semaphoreLimit;
 * - после завершения анализа activeFiles равно нулю;
 * - один повреждённый файл не мешает обработке остальных;
 * - в программе нет дедлока.
 *
 * В доказательстве отсутствия дедлока укажите порядок захвата мониторов и
 * семафоров. Отдельно объясните, может ли поток ждать acquire(), удерживая
 * synchronized-монитор, и почему разрешение семафора возвращается при исключении.
 * Объясните, почему volatile нужен для флага остановки.
 *
 * НЕБОЛЬШИЕ ЭКСПЕРИМЕНТЫ ПОСЛЕ РЕАЛИЗАЦИИ:
 * - сравнить 1 и 4 рабочих потока;
 * - сравнить лимит семафора 1 и 4;
 * - сравнить глобальную блокировку и блокировки по URL;
 * - добавить одну повреждённую строку или один повреждённый файл и проверить,
 *   что остальные данные всё равно попадают в отчёт.
 * Эксперименты не требуют дополнительной архитектуры или новых классов.
 */
public class LogAnalysisAssignment {

    public record LogEntry(
            String method,
            String path,
            int status,
            long responseBytes,
            long durationMillis
    ) {}

    public record LogFile(Path path) {}

    public record PathStatistics(
            long requests,
            long successful,
            long errors,
            long responseBytes,
            long totalDurationMillis,
            long maxDurationMillis
    ) {}

    public record Report(
            long totalRequests,
            long successfulRequests,
            long errorRequests,
            long responseBytes,
            long maxDurationMillis
    ) {}

    /**
     * Возвращает отдельный итератор для чтения строк одного файла.
     *
     * ВАЖНО: Iterator<String> потоконебезопасен. Один экземпляр нельзя
     * одновременно читать из нескольких потоков. Для каждого потока нужен
     * отдельный вызов LogFileLines.iterator(path).
     */
    public static final class LogFileLines {
        private LogFileLines() {}

        // Итератор, который можно закрыть. Нужен, чтобы при досрочной остановке
        // не оставлять открытый файл (сам итератор закрывается только в конце файла).
        public interface Lines extends Iterator<String>, AutoCloseable {
            @Override
            void close();
        }

        public static Lines iterator(Path path) {
            try {
                BufferedReader reader = Files.newBufferedReader(
                        path,
                        StandardCharsets.UTF_8
                );

                return new Lines() {
                    private String nextLine = readNext();
                    private boolean closed;

                    private String readNext() {
                        try {
                            String line = reader.readLine();
                            if (line == null) {
                                close();
                            }
                            return line;
                        } catch (IOException exception) {
                            close();
                            throw new RuntimeException(
                                    "Cannot read " + path,
                                    exception
                            );
                        }
                    }

                    @Override
                    public void close() {
                        if (closed) {
                            return;
                        }
                        closed = true;
                        try {
                            reader.close();
                        } catch (IOException exception) {
                            throw new RuntimeException(
                                    "Cannot close " + path,
                                    exception
                            );
                        }
                    }

                    @Override
                    public boolean hasNext() {
                        return nextLine != null;
                    }

                    @Override
                    public String next() {
                        if (nextLine == null) {
                            throw new NoSuchElementException();
                        }
                        String result = nextLine;
                        nextLine = readNext();
                        return result;
                    }
                };
            } catch (IOException exception) {
                throw new RuntimeException(
                        "Cannot open " + path,
                        exception
                );
            }
        }
    }

    /** Генерирует реальные текстовые файлы с детерминированными данными. */
    public static final class GenerateLogs {
        private static final String[] METHODS = {
                "GET", "POST", "PUT", "DELETE"
        };
        private static final String[] PATHS = {
                "/", "/index.html", "/login", "/api/users",
                "/api/orders", "/api/products", "/search", "/health"
        };
        private static final int[] STATUSES = {
                200, 200, 200, 201, 204,
                301, 400, 401, 404, 500
        };

        private GenerateLogs() {}

        public static void generate(
                Path outputDirectory,
                int fileCount,
                int linesPerFile
        ) throws IOException {
            if (fileCount < 0 || linesPerFile < 0) {
                throw new IllegalArgumentException(
                        "Counts must be non-negative"
                );
            }

            Files.createDirectories(outputDirectory);
            Random random = new Random(42);

            for (int fileId = 0; fileId < fileCount; fileId++) {
                Path file = outputDirectory.resolve(
                        "access-%03d.log".formatted(fileId)
                );

                try (BufferedWriter writer = Files.newBufferedWriter(
                        file,
                        StandardCharsets.UTF_8
                )) {
                    for (int line = 0; line < linesPerFile; line++) {
                        String method =
                                METHODS[random.nextInt(METHODS.length)];
                        String path =
                                PATHS[random.nextInt(PATHS.length)];
                        int status =
                                STATUSES[random.nextInt(STATUSES.length)];
                        long responseBytes =
                                100L + random.nextInt(100_000);
                        long durationMillis =
                                1L + random.nextInt(500);

                        writer.write(
                                "%s %s %d %d %d%n".formatted(
                                        method,
                                        path,
                                        status,
                                        responseBytes,
                                        durationMillis
                                )
                        );
                    }
                }
            }
        }
    }

    // ---------------- 1. Разбор строки ----------------

    // Строка должна быть вида: METHOD PATH STATUS BYTES MILLIS
    // Статус меньше 200 считаем ошибкой формата: он не успешный и не ошибочный,
    // и тогда requests != successful + errors.
    static LogEntry parseLine(String line) {
        String[] parts = line.trim().split("\\s+");
        if (parts.length != 5) {
            throw new IllegalArgumentException(
                    "ожидалось 5 полей, а нашлось " + parts.length + ": '" + line + "'");
        }
        try {
            int status = Integer.parseInt(parts[2]);
            long bytes = Long.parseLong(parts[3]);
            long millis = Long.parseLong(parts[4]);
            if (status < 200 || bytes < 0 || millis < 0) {
                throw new IllegalArgumentException("плохие значения: '" + line + "'");
            }
            return new LogEntry(parts[0], parts[1], status, bytes, millis);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("в числовом поле не число: '" + line + "'", e);
        }
    }

    // ---------------- Результат анализа ----------------

    public record AnalysisResult(
            Report report,
            Map<String, PathStatistics> byPath,
            List<String> failedFiles,
            int skippedFiles,
            boolean stopped,
            long linesRead,
            long badLines,
            int maxActiveFiles
    ) {}

    static Report makeReport(Map<String, PathStatistics> byPath) {
        long requests = 0, ok = 0, errors = 0, bytes = 0, max = 0;
        for (PathStatistics p : byPath.values()) {
            requests += p.requests();
            ok += p.successful();
            errors += p.errors();
            bytes += p.responseBytes();
            max = Math.max(max, p.maxDurationMillis());
        }
        return new Report(requests, ok, errors, bytes, max);
    }

    // Обычный изменяемый счётчик для одного URL. Сам он не синхронизирован,
    // синхронизацию делает тот, кто его использует.
    static class PathCounter {
        long requests, successful, errors, bytes, totalTime, maxTime;

        void add(LogEntry e) {
            requests++;
            if (e.status() < 400) {
                successful++;
            } else {
                errors++;
            }
            bytes += e.responseBytes();
            totalTime += e.durationMillis();
            maxTime = Math.max(maxTime, e.durationMillis());
        }

        PathStatistics toStatistics() {
            return new PathStatistics(requests, successful, errors, bytes, totalTime, maxTime);
        }
    }

    // ---------------- 2. Последовательный отчёт ----------------

    static AnalysisResult analyzeSequential(List<LogFile> files) {
        Map<String, PathCounter> counters = new HashMap<>();
        List<String> failed = new ArrayList<>();
        long linesRead = 0;
        long badLines = 0;

        for (LogFile file : files) {
            try (var lines = LogFileLines.iterator(file.path())) {
                while (lines.hasNext()) {
                    String line = lines.next();
                    linesRead++;
                    LogEntry entry;
                    try {
                        entry = parseLine(line);
                    } catch (IllegalArgumentException e) {
                        badLines++;
                        continue;
                    }
                    counters.computeIfAbsent(entry.path(), p -> new PathCounter()).add(entry);
                }
            } catch (RuntimeException e) {
                failed.add(describe(file, e));
            }
        }

        Map<String, PathStatistics> byPath = new TreeMap<>();
        counters.forEach((path, c) -> byPath.put(path, c.toStatistics()));
        return new AnalysisResult(makeReport(byPath), byPath, failed, 0, false,
                linesRead, badLines, 0);
    }

    static String describe(LogFile file, Exception e) {
        String text = file.path() + ": " + e.getMessage();
        if (e.getCause() != null) {
            text += " (" + e.getCause() + ")";
        }
        return text;
    }

    // ---------------- 4. Потокобезопасная статистика ----------------

    interface Stats {
        void add(LogEntry entry);

        Map<String, PathStatistics> result();
    }

    // Вариант 1: один замок на всё.
    static class GlobalLockStats implements Stats {
        private final Map<String, PathCounter> map = new HashMap<>();

        @Override
        public synchronized void add(LogEntry entry) {
            map.computeIfAbsent(entry.path(), p -> new PathCounter()).add(entry);
        }

        @Override
        public synchronized Map<String, PathStatistics> result() {
            Map<String, PathStatistics> res = new TreeMap<>();
            map.forEach((path, c) -> res.put(path, c.toStatistics()));
            return res;
        }
    }

    // Вариант 2: ConcurrentHashMap и свой замок у каждого URL.
    static class PerUrlStats implements Stats {
        private final ConcurrentHashMap<String, PathCounter> map = new ConcurrentHashMap<>();

        @Override
        public void add(LogEntry entry) {
            PathCounter c = map.computeIfAbsent(entry.path(), p -> new PathCounter());
            synchronized (c) {
                c.add(entry);
            }
        }

        @Override
        public Map<String, PathStatistics> result() {
            Map<String, PathStatistics> res = new TreeMap<>();
            for (var e : map.entrySet()) {
                PathCounter c = e.getValue();
                synchronized (c) {
                    res.put(e.getKey(), c.toStatistics());
                }
            }
            return res;
        }
    }

    // ---------------- 6. Atomic-счётчики ----------------

    static class Counters {
        final AtomicInteger filesFinished = new AtomicInteger();
        final AtomicInteger filesSkipped = new AtomicInteger();
        final AtomicInteger readErrors = new AtomicInteger();
        final AtomicLong linesRead = new AtomicLong();
        final AtomicLong goodLines = new AtomicLong();
        final AtomicLong badLines = new AtomicLong();
        final AtomicInteger activeFiles = new AtomicInteger();
        final AtomicInteger maxActiveFiles = new AtomicInteger();

        void fileStarted() {
            int now = activeFiles.incrementAndGet();
            int oldMax = maxActiveFiles.get();
            while (now > oldMax && !maxActiveFiles.compareAndSet(oldMax, now)) {
                oldMax = maxActiveFiles.get();
            }
        }

        void fileEnded() {
            activeFiles.decrementAndGet();
        }
    }

    // ---------------- 3, 5, 7, 8, 9. Параллельный анализ ----------------

    static class ParallelAnalyzer {
        private final int workers;
        private final int permits;
        private final Semaphore semaphore;
        private final Stats stats;
        private final Queue<String> failedFiles = new ConcurrentLinkedQueue<>();
        final Counters counters = new Counters();
        private volatile boolean stopRequested = false;

        ParallelAnalyzer(int workers, int permits, Stats stats) {
            this.workers = workers;
            this.permits = permits;
            this.semaphore = new Semaphore(permits);
            this.stats = stats;
        }

        void stop() {
            stopRequested = true;
        }

        int freePermits() {
            return semaphore.availablePermits();
        }

        AnalysisResult analyze(List<LogFile> files) throws InterruptedException {
            CountDownLatch latch = new CountDownLatch(files.size());
            ExecutorService pool = Executors.newFixedThreadPool(workers);
            try {
                for (LogFile file : files) {
                    pool.execute(() -> processFile(file, latch));
                }
                latch.await();
            } finally {
                pool.shutdown();
                if (!pool.awaitTermination(1, TimeUnit.MINUTES)) {
                    pool.shutdownNow();
                }
            }

            Map<String, PathStatistics> byPath = stats.result();
            return new AnalysisResult(
                    makeReport(byPath),
                    byPath,
                    new ArrayList<>(failedFiles),
                    counters.filesSkipped.get(),
                    stopRequested,
                    counters.linesRead.get(),
                    counters.badLines.get(),
                    counters.maxActiveFiles.get());
        }

        private void processFile(LogFile file, CountDownLatch latch) {
            try {
                semaphore.acquire();
                try {
                    counters.fileStarted();
                    try {
                        readFile(file);
                    } finally {
                        counters.fileEnded();
                    }
                } finally {
                    semaphore.release();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                fail(file, e);
            } catch (RuntimeException e) {
                fail(file, e);
            } finally {
                counters.filesFinished.incrementAndGet();
                latch.countDown();
            }
        }

        private void fail(LogFile file, Exception e) {
            counters.readErrors.incrementAndGet();
            failedFiles.add(describe(file, e));
        }

        // Политика: плохая строка пропускается, файл читается дальше.
        // Если файл не открылся или упал посреди чтения, он попадает в failedFiles,
        // а строки, которые успели прочитать до ошибки, остаются в статистике.
        private void readFile(LogFile file) {
            if (stopRequested) {
                counters.filesSkipped.incrementAndGet();
                return;
            }
            try (var lines = LogFileLines.iterator(file.path())) {
                while (lines.hasNext()) {
                    if (stopRequested) {
                        return;
                    }
                    String line = lines.next();
                    counters.linesRead.incrementAndGet();
                    try {
                        stats.add(parseLine(line));
                        counters.goodLines.incrementAndGet();
                    } catch (IllegalArgumentException e) {
                        long n = counters.badLines.incrementAndGet();
                        if (n <= 10) {
                            System.err.println("Плохая строка в "
                                    + file.path().getFileName() + ": " + e.getMessage());
                        }
                    }
                }
            }
        }
    }

    static List<LogFile> listFiles(Path directory) throws IOException {
        try (var paths = Files.list(directory)) {
            return paths
                    .filter(p -> p.getFileName().toString().endsWith(".log"))
                    .sorted()
                    .map(LogFile::new)
                    .toList();
        }
    }

    static void printResult(AnalysisResult r) {
        Report rep = r.report();
        System.out.println("Запросов: " + rep.totalRequests()
                + " (успешных " + rep.successfulRequests()
                + ", ошибок " + rep.errorRequests() + ")");
        System.out.println("Байт ответов: " + rep.responseBytes());
        System.out.println("Максимальное время: " + rep.maxDurationMillis() + " мс");
        System.out.println("Прочитано строк: " + r.linesRead() + ", плохих строк: " + r.badLines());
        for (var e : r.byPath().entrySet()) {
            PathStatistics p = e.getValue();
            System.out.printf("  %-16s запросов=%d ok=%d err=%d байт=%d всего=%d мс макс=%d мс%n",
                    e.getKey(), p.requests(), p.successful(), p.errors(),
                    p.responseBytes(), p.totalDurationMillis(), p.maxDurationMillis());
        }
        if (!r.failedFiles().isEmpty()) {
            System.out.println("НЕ УДАЛОСЬ обработать файлов: " + r.failedFiles().size());
            r.failedFiles().forEach(f -> System.out.println("  " + f));
        }
        if (r.stopped()) {
            System.out.println("Анализ остановлен досрочно, отчёт частичный. Файлов не начато: "
                    + r.skippedFiles());
        }
    }

    // ---------------- Проверки ----------------

    static void require(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }

    static void checkInvariants(AnalysisResult r) {
        for (var e : r.byPath().entrySet()) {
            PathStatistics p = e.getValue();
            require(p.requests() == p.successful() + p.errors(), "requests != ok + err у " + e.getKey());
            require(p.maxDurationMillis() <= p.totalDurationMillis(), "max > total у " + e.getKey());
        }
    }

    static AnalysisResult runParallel(List<LogFile> files, int workers, int permits, Stats stats)
            throws InterruptedException {
        ParallelAnalyzer analyzer = new ParallelAnalyzer(workers, permits, stats);
        AnalysisResult r = analyzer.analyze(files);
        require(analyzer.counters.activeFiles.get() == 0, "после анализа activeFiles != 0");
        require(analyzer.freePermits() == permits, "разрешения семафора не вернулись");
        require(r.maxActiveFiles() <= permits, "одновременно читалось больше файлов, чем разрешений");
        return r;
    }

    private static void verify() throws Exception {
        Path dir = Files.createTempDirectory("log-verify-");
        GenerateLogs.generate(dir, 12, 2000);
        List<LogFile> files = listFiles(dir);

        // 1. Полный запуск: параллельный отчёт совпадает с последовательным
        AnalysisResult expected = analyzeSequential(files);
        checkInvariants(expected);
        require(expected.linesRead() == 24000, "в эталоне не 24000 строк");
        require(expected.report().totalRequests() == 24000, "в эталоне не 24000 запросов");

        for (int workers : new int[]{1, 4}) {
            for (int permits : new int[]{1, 4}) {
                for (Stats stats : new Stats[]{new GlobalLockStats(), new PerUrlStats()}) {
                    AnalysisResult r = runParallel(files, workers, permits, stats);
                    String name = stats.getClass().getSimpleName() + " workers=" + workers + " permits=" + permits;
                    require(r.byPath().equals(expected.byPath()), "статистика не совпала: " + name);
                    require(r.report().equals(expected.report()), "отчёт не совпал: " + name);
                    require(r.linesRead() == 24000 && r.badLines() == 0, "счётчики строк: " + name);
                    require(r.failedFiles().isEmpty() && !r.stopped(), "лишние ошибки: " + name);
                    checkInvariants(r);
                }
            }
        }
        System.out.println("OK: параллельный отчёт совпадает с последовательным (8 конфигураций)");

        // 2. Две плохие строки в одном файле
        Path bad = dir.resolve("access-000.log");
        Files.writeString(bad, "GET /broken oops\nGET /x abc 1 1\n",
                StandardOpenOption.APPEND);
        AnalysisResult withBadLines = runParallel(files, 4, 2, new PerUrlStats());
        require(withBadLines.badLines() == 2, "плохих строк должно быть 2");
        require(withBadLines.report().equals(expected.report()), "плохие строки испортили отчёт");
        require(withBadLines.failedFiles().isEmpty(), "файл с плохой строкой не должен падать");
        System.out.println("OK: плохие строки пропущены, остальные данные на месте");

        // 3. Один нечитаемый файл и один несуществующий
        Files.write(dir.resolve("access-998.log"), new byte[]{(byte) 0xFF, (byte) 0xFE, (byte) 0xFA, '\n'});
        List<LogFile> withBroken = new ArrayList<>(listFiles(dir));
        withBroken.add(new LogFile(dir.resolve("net-takogo-faila.log")));
        AnalysisResult broken = runParallel(withBroken, 4, 2, new GlobalLockStats());
        require(broken.failedFiles().size() == 2, "должно быть 2 упавших файла");
        require(broken.report().equals(expected.report()), "упавшие файлы испортили отчёт");
        require(broken.byPath().equals(expected.byPath()), "статистика по URL испорчена упавшими файлами");
        System.out.println("OK: упавшие файлы видны в результате, остальные обработаны");
        broken.failedFiles().forEach(f -> System.out.println("    " + f));

        // 4. Досрочная остановка
        Path bigDir = Files.createTempDirectory("log-verify-big-");
        GenerateLogs.generate(bigDir, 10, 100_000);
        List<LogFile> bigFiles = listFiles(bigDir);
        ParallelAnalyzer analyzer = new ParallelAnalyzer(2, 1, new PerUrlStats());
        Thread watcher = new Thread(() -> {
            while (analyzer.counters.linesRead.get() < 50_000) {
                Thread.onSpinWait();
            }
            analyzer.stop();
        });
        watcher.start();
        AnalysisResult stopped = analyzer.analyze(bigFiles);
        watcher.join();
        require(stopped.stopped(), "флаг stopped не выставлен");
        require(stopped.linesRead() < 1_000_000, "остановка не сработала, прочитано всё");
        require(stopped.linesRead() == analyzer.counters.goodLines.get() + analyzer.counters.badLines.get(),
                "прочитанная строка не была обработана");
        require(stopped.report().totalRequests() == analyzer.counters.goodLines.get(),
                "в отчёте не те строки, что посчитаны");
        require(analyzer.counters.activeFiles.get() == 0 && analyzer.freePermits() == 1,
                "после остановки остались активные файлы или потеряно разрешение");
        checkInvariants(stopped);
        System.out.println("OK: остановка, прочитано " + stopped.linesRead()
                + " из 1000000 строк, не начато файлов: " + stopped.skippedFiles());
    }

    // ---------------- Эксперименты ----------------

    static void writeFiles(Path dir, int files, int lines, int urls, boolean oneHotUrl) throws IOException {
        Files.createDirectories(dir);
        Random random = new Random(1);
        for (int f = 0; f < files; f++) {
            try (BufferedWriter out = Files.newBufferedWriter(dir.resolve("access-%03d.log".formatted(f)))) {
                for (int i = 0; i < lines; i++) {
                    String path;
                    if (oneHotUrl && random.nextInt(100) < 99) {
                        path = "/api/users";
                    } else {
                        path = "/page/" + random.nextInt(urls);
                    }
                    int status = random.nextInt(10) == 0 ? 500 : 200;
                    out.write("GET " + path + " " + status + " "
                            + (100 + random.nextInt(5000)) + " " + (1 + random.nextInt(300)) + "\n");
                }
            }
        }
    }

    interface Job {
        void run() throws Exception;
    }

    // лучшее время из трёх запусков, в мс
    static long bestTime(Job job) throws Exception {
        long best = Long.MAX_VALUE;
        for (int i = 0; i < 3; i++) {
            long start = System.nanoTime();
            job.run();
            best = Math.min(best, (System.nanoTime() - start) / 1_000_000);
        }
        return best;
    }

    private static void bench(int fileCount, int lines) throws Exception {
        System.out.println("Ядер у процессора: " + Runtime.getRuntime().availableProcessors());
        System.out.println("Файлов: " + fileCount + ", строк в файле: " + lines + ", время — лучшее из 3");

        Path normal = Files.createTempDirectory("bench-normal-");
        GenerateLogs.generate(normal, fileCount, lines);
        Path hot = Files.createTempDirectory("bench-hot-");
        writeFiles(hot, fileCount, lines, 10, true);
        Path many = Files.createTempDirectory("bench-many-");
        writeFiles(many, fileCount, lines, 10_000, false);

        List<LogFile> files = listFiles(normal);
        System.out.println();
        System.out.println("Данные: 8 разных URL");
        System.out.println("  последовательно:                   " + bestTime(() -> analyzeSequential(files)) + " мс");
        for (int[] c : new int[][]{{1, 1}, {4, 1}, {4, 4}}) {
            long t = bestTime(() -> runParallel(files, c[0], c[1], new PerUrlStats()));
            System.out.println("  потоков " + c[0] + ", семафор " + c[1] + ", замки по URL:   " + t + " мс");
        }

        for (var data : new Object[][]{{"8 разных URL", normal}, {"99% запросов на один URL", hot}, {"10000 разных URL", many}}) {
            List<LogFile> list = listFiles((Path) data[1]);
            long global = bestTime(() -> runParallel(list, 4, 4, new GlobalLockStats()));
            long perUrl = bestTime(() -> runParallel(list, 4, 4, new PerUrlStats()));
            System.out.println();
            System.out.println("Данные: " + data[0] + " (4 потока, семафор 4)");
            System.out.println("  один общий замок:  " + global + " мс");
            System.out.println("  замки по URL:      " + perUrl + " мс");
        }
    }

    /** Проверяет генератор и чтение строк без участия студенческого анализатора. */
    private static void selfCheck() throws IOException {
        Path directory = Files.createTempDirectory("log-assignment-");
        GenerateLogs.generate(directory, 3, 7);

        int files = 0;
        int lines = 0;

        try (var paths = Files.list(directory)) {
            for (Path path : paths.toList()) {
                files++;
                Iterator<String> iterator = LogFileLines.iterator(path);

                while (iterator.hasNext()) {
                    String line = iterator.next();
                    String[] fields = line.trim().split("\\s+");
                    if (fields.length != 5) {
                        throw new AssertionError(
                                "Unexpected line: " + line
                        );
                    }
                    Integer.parseInt(fields[2]);
                    Long.parseLong(fields[3]);
                    Long.parseLong(fields[4]);
                    lines++;
                }
            }
        }

        if (files != 3 || lines != 21) {
            throw new AssertionError(
                    "Expected 3 files and 21 lines, got "
                            + files + " files and " + lines + " lines"
            );
        }

        System.out.println(
                "OK: generated " + files
                        + " files and read " + lines + " lines"
        );
    }

    public static void main(String[] args) throws Exception {
        if (args.length > 0 && args[0].equals("generate")) {
            Path directory = Path.of(
                    args.length > 1 ? args[1] : "logs"
            );
            int fileCount = args.length > 2
                    ? Integer.parseInt(args[2])
                    : 20;
            int linesPerFile = args.length > 3
                    ? Integer.parseInt(args[3])
                    : 1_000;

            GenerateLogs.generate(
                    directory,
                    fileCount,
                    linesPerFile
            );

            System.out.println(
                    "Generated " + fileCount
                            + " files in " + directory
            );
            return;
        }

        if (args.length > 0 && args[0].equals("check")) {
            selfCheck();
            return;
        }

        if (args.length > 0 && args[0].equals("verify")) {
            verify();
            return;
        }

        if (args.length > 0 && args[0].equals("bench")) {
            int fileCount = args.length > 1 ? Integer.parseInt(args[1]) : 16;
            int lines = args.length > 2 ? Integer.parseInt(args[2]) : 50_000;
            bench(fileCount, lines);
            return;
        }

        if (args.length > 0 && args[0].equals("analyze")) {
            Path directory = Path.of(args.length > 1 ? args[1] : "logs");
            int workers = args.length > 2 ? Integer.parseInt(args[2]) : 4;
            int permits = args.length > 3 ? Integer.parseInt(args[3]) : 2;
            boolean global = args.length > 4 && args[4].equals("global");
            Stats stats = global ? new GlobalLockStats() : new PerUrlStats();
            printResult(runParallel(listFiles(directory), workers, permits, stats));
            return;
        }

        System.out.println(
                "Usage: java LogAnalysisAssignment "
                        + "generate <directory> <files> <lines>"
        );
        System.out.println(
                "       java LogAnalysisAssignment check"
        );
        System.out.println(
                "       java LogAnalysisAssignment analyze <directory> <workers> <permits> [global|url]"
        );
        System.out.println(
                "       java LogAnalysisAssignment verify"
        );
        System.out.println(
                "       java LogAnalysisAssignment bench [files] [lines]"
        );
    }
}
