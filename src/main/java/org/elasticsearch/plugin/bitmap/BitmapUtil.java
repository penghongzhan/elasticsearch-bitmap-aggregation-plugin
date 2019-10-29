package org.elasticsearch.plugin.bitmap;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author : zhanpenghong
 * @date : 2019-10-24 20:54
 */
public class BitmapUtil {

    private static final Logger SLOG = LoggerFactory.getLogger(BitmapUtil.class);

    public static RoaringBitmap deserializeBitmap(byte[] arr) {
        RoaringBitmap roaringBitmap = new RoaringBitmap();
        try(ByteArrayInputStream bai = new ByteArrayInputStream(arr);
                ObjectInputStream inputStream = new ObjectInputStream(bai)) {
            roaringBitmap.deserialize(inputStream);
        } catch (IOException e) {
            SLOG.error("deserializeBitmap error", e);
        }
        return roaringBitmap;
    }

    public static byte[] serializeBitmap(RoaringBitmap roaringBitmap) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            boolean runOptimize = roaringBitmap.runOptimize();
            SLOG.debug("run optimize result: [{}]", runOptimize);
            roaringBitmap.serialize(oos);
            oos.flush();
            return baos.toByteArray();
        } catch (Exception e) {
            SLOG.error("serializeBitmap error", e);
        }
        return null;
    }
}
