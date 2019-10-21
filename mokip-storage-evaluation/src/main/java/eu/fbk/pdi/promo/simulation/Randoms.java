package eu.fbk.pdi.promo.simulation;

import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Random;

public class Randoms {

    private static final Random RANDOM = new Random(0);

    public static String randDate() {

        final int year = Randoms.randIntBetween(1900, 2010);
        final int month = Randoms.randIntBetween(0, 11);

        final GregorianCalendar gc = new GregorianCalendar(year, month, 1);
        final int day = Randoms.randIntBetween(1, gc.getActualMaximum(Calendar.DAY_OF_MONTH));
        gc.set(year, month, day);
        return gc.get(Calendar.YEAR) + "-" + gc.get(Calendar.MONTH) + "-"
        + gc.get(Calendar.DAY_OF_MONTH);
    }

    public static String randUUID() {

        final int u0 = RANDOM.nextInt();
        final int u1 = RANDOM.nextInt();
        final int u2 = RANDOM.nextInt();
        final int u3 = RANDOM.nextInt();

        return String.format("%08x-%04x-4%03x-a%03x-%04x%08x", u0, u1 >>> 16, u1 & 0xFFF,
                u2 >>> 16 & 0xFFF, u2 & 0xFFFF, u3);
    }

    public static int randInt(final int max) {
        return RANDOM.nextInt(max);
    }

    public static int randIntBetween(final int start, final int end) {
        return start + (int) Math.round(RANDOM.nextDouble() * (end - start));
    }

}
