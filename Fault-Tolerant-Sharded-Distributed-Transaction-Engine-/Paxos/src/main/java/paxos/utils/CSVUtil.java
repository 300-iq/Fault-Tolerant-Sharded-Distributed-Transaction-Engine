package paxos.utils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;



//Took help of Chat gpt for CSV parser.
public final class CSVUtil {

    private CSVUtil() {}

    public static record Txn(String sender, String receiver, int amount) {}
    public static record TestSet(int setNo,
                                 List<Txn> txns,
                                 List<String> liveNodes,
                                 List<List<Txn>> segments,
                                 int lfCount) {}

    public static List<TestSet> parse(Path csv) throws IOException {
        List<String> lines = Files.readAllLines(csv);
        if (lines.isEmpty()) {
            return List.of();
        }

        boolean rowFormat = lines.stream()
                .map(line -> line == null ? "" : line.trim())
                .anyMatch(line -> !line.isEmpty()
                        && Character.isDigit(line.charAt(0))
                        && line.contains(",")
                        && line.contains("(")
                        && line.contains(")"));

        if (rowFormat) {
            return parseRowFormat(lines);
        }
        return parseLegacyFormat(lines);
    }

    private static List<TestSet> parseRowFormat(List<String> rawLines) {
        Map<Integer, List<Txn>> txnsBySet = new LinkedHashMap<>();
        Map<Integer, List<String>> liveBySet = new LinkedHashMap<>();
        Map<Integer, List<List<Txn>>> segsBySet = new LinkedHashMap<>();
        Integer currentSet = null;

        for (String raw : rawLines) {
            if (raw == null) continue;
            String line = raw.trim();
            if (line.isEmpty()) continue;
            String lower = line.toLowerCase(Locale.ROOT);
            if (lower.startsWith("set number")) continue;

            List<String> cols = splitTopLevel(line);
            if (cols.isEmpty()) continue;

            String col0 = cols.get(0).trim();
            Integer setNo = parseInt(unquote(col0));
            if (setNo != null) {
                currentSet = setNo;
                txnsBySet.computeIfAbsent(currentSet, k -> new ArrayList<>());
                segsBySet.computeIfAbsent(currentSet, k -> new ArrayList<>());
            }
            if (currentSet == null) continue;

            String txnCol = cols.size() > 1 ? unquote(cols.get(1).trim()) : "";
            if (!txnCol.isEmpty()) {
                if (txnCol.equalsIgnoreCase("LF")) {

                    List<List<Txn>> segs = segsBySet.computeIfAbsent(currentSet, k -> new ArrayList<>());

                    if (segs.isEmpty() || !segs.get(segs.size() - 1).isEmpty()) {
                        segs.add(new ArrayList<>());
                    }
                } else {
                    parseTxn(currentSet, txnCol, txnsBySet);

                    List<List<Txn>> segs = segsBySet.computeIfAbsent(currentSet, k -> new ArrayList<>());
                    if (segs.isEmpty()) segs.add(new ArrayList<>());
                    List<Txn> cur = segs.get(segs.size() - 1);

                    Txn t = parseTxnValue(txnCol);
                    if (t != null) {
                        cur.add(t);
                    }
                }
            }

            String liveCol = cols.size() > 2 ? unquote(cols.get(2).trim()) : "";
            if (!liveCol.isEmpty() && currentSet != null) {
                List<String> parsed = parseLiveNodes(liveCol);
                liveBySet.put(currentSet, parsed);
            }
        }

        return buildOutput(txnsBySet, liveBySet, segsBySet);
    }

    private static List<TestSet> parseLegacyFormat(List<String> lines) {
        List<TestSet> out = new ArrayList<>();
        List<Txn> txns = new ArrayList<>();
        List<String> live = new ArrayList<>();
        List<List<Txn>> segments = new ArrayList<>();
        segments.add(new ArrayList<>());
        int setNo = -1;

        for (String raw : lines) {
            if (raw == null) continue;
            String line = raw.trim();
            if (line.isEmpty()) continue;

            if (Character.isDigit(line.charAt(0))) {
                if (setNo != -1) {
                    out.add(new TestSet(setNo, List.copyOf(txns), List.copyOf(live), deepCopySegments(segments), Math.max(0, segments.size()-1)));
                    txns.clear();
                    live.clear();
                    segments.clear();
                    segments.add(new ArrayList<>());
                }
                String digits = line.replaceAll("[^0-9]", "");
                setNo = digits.isEmpty() ? -1 : Integer.parseInt(digits);
            } else if (line.equalsIgnoreCase("LF")) {
                if (segments.isEmpty() || !segments.get(segments.size()-1).isEmpty()) {
                    segments.add(new ArrayList<>());
                }
            } else if (line.startsWith("(")) {
                parseTxnInto(line, txns);

                if (segments.isEmpty()) segments.add(new ArrayList<>());
                Txn t = parseTxnValue(line);
                if (t != null) segments.get(segments.size()-1).add(t);
            } else if (line.startsWith("[")) {
                live.addAll(parseLiveNodes(line));
            }
        }
        if (setNo != -1) {
            out.add(new TestSet(setNo, List.copyOf(txns), List.copyOf(live), deepCopySegments(segments), Math.max(0, segments.size()-1)));
        }
        return out;
    }

    private static List<TestSet> buildOutput(Map<Integer, List<Txn>> txnsBySet, Map<Integer, List<String>> liveBySet, Map<Integer, List<List<Txn>>> segsBySet) {
        List<TestSet> out = new ArrayList<>();
        for (Map.Entry<Integer, List<Txn>> entry : txnsBySet.entrySet()) {
            int setNo = entry.getKey();
            List<Txn> txns = entry.getValue();
            List<String> live = liveBySet.getOrDefault(setNo, List.of());
            List<List<Txn>> segs = segsBySet.getOrDefault(setNo, List.of());

            List<List<Txn>> normalized = new ArrayList<>();
            if (segs.isEmpty()) {
                normalized.add(new ArrayList<>(txns));
            } else {
                for (List<Txn> s : segs) normalized.add(new ArrayList<>(s));
            }
            int lfCount = Math.max(0, normalized.size() - 1);
            out.add(new TestSet(setNo, List.copyOf(txns), List.copyOf(live), List.copyOf(normalized), lfCount));
        }
        return out;
    }

    private static void parseTxn(int setNo, String txnCol, Map<Integer, List<Txn>> txnsBySet) {
        List<Txn> list = txnsBySet.computeIfAbsent(setNo, k -> new ArrayList<>());
        parseTxnInto(txnCol, list);
    }

    private static void parseTxnInto(String txnCol, List<Txn> into) {
        String body = txnCol.replace("(", "").replace(")", "").trim();
        if (body.isEmpty()) return;
        String[] parts = body.split(",");
        if (parts.length == 1) {
            String sender = parts[0].trim();
            if (sender.isEmpty()) return;
            into.add(new Txn(sender, "", 0));
            return;
        }
        if (parts.length < 3) return;
        String sender = parts[0].trim();
        String receiver = parts[1].trim();
        Integer amount = parseInt(parts[2]);
        if (sender.isEmpty() || receiver.isEmpty() || amount == null) return;
        into.add(new Txn(sender, receiver, amount));
    }

    private static Txn parseTxnValue(String txnCol) {
        String body = unquote(txnCol).replace("(", "").replace(")", "").trim();
        if (body.isEmpty()) return null;
        String[] parts = body.split(",");
        if (parts.length == 1) {
            String sender = unquote(parts[0].trim());
            if (sender.isEmpty()) return null;
            return new Txn(sender, "", 0);
        }
        if (parts.length < 3) return null;
        String sender = unquote(parts[0].trim());
        String receiver = unquote(parts[1].trim());
        Integer amount = parseInt(parts[2]);
        if (sender.isEmpty() || receiver.isEmpty() || amount == null) return null;
        return new Txn(sender, receiver, amount);
    }

    private static List<String> parseLiveNodes(String col) {
        String body = col;
        int start = col.indexOf('[');
        int end = col.lastIndexOf(']');
        if (start != -1 && end != -1 && end > start) {
            body = col.substring(start + 1, end);
        }
        if (body.isBlank()) {
            return List.of();
        }
        String[] tokens = body.split(",");
        List<String> out = new ArrayList<>(tokens.length);
        for (String token : tokens) {
            String trimmed = token.trim();
            if (!trimmed.isEmpty()) {
                out.add(trimmed);
            }
        }
        return out;
    }

    private static List<String> splitTopLevel(String line) {
        List<String> cols = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        int paren = 0;
        int bracket = 0;
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (c == ',' && paren == 0 && bracket == 0) {
                cols.add(current.toString());
                current.setLength(0);
                continue;
            }
            if (c == '(') paren++;
            if (c == ')') paren = Math.max(0, paren - 1);
            if (c == '[') bracket++;
            if (c == ']') bracket = Math.max(0, bracket - 1);
            current.append(c);
        }
        if (current.length() > 0) {
            cols.add(current.toString());
        }
        return cols;
    }

    private static Integer parseInt(String value) {
        if (value == null) return null;
        String digits = value.replaceAll("[^0-9-]", "").trim();
        if (digits.isEmpty()) {
            return null;
        }
        try {
            return Integer.parseInt(digits);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static List<List<Txn>> deepCopySegments(List<List<Txn>> segs) {
        List<List<Txn>> out = new ArrayList<>(segs.size());
        for (List<Txn> s : segs) out.add(new ArrayList<>(s));
        return out;
    }

    private static String unquote(String s) {
        if (s == null || s.length() < 2) return s;
        char first = s.charAt(0);
        char last = s.charAt(s.length() - 1);
        if ((first == '"' && last == '"') || (first == '\'' && last == '\'')) {
            return s.substring(1, s.length() - 1).trim();
        }
        return s;
    }
}
