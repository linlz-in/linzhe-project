import com.lz.DatasecureUtils;

import javax.crypto.spec.SecretKeySpec;

/**
 * @Package:
 * @Author: lz
 * @Date: 2025/8/4 20:46
 * @version: 1.8
 */
public class TestDEcode {
    public static void main(String[] args) {
        String str = "{\"UPEXGMSGREALLOCATION\":{\"dateTime\":\"20250730121804\",\"altitude\":48,\"vec3\":23902,\"alarm\":{\"terminalLcdError\":0,\"earlyWarning\":0,\"terminalMainPowerUnderVoltage\":0,\"inOutRoute\":0,\"oilError\":0,\"overSpeed\":0,\"emergencyAlarm\":0,\"rolloverWarning\":0,\"illegalMove\":0,\"inOutArea\":0,\"gnssAntennaDisconnect\":0,\"laneDepartureError\":0,\"stopTimeout\":0,\"vssError\":0,\"ttsModuleError\":0,\"fatigueDriving\":0,\"gnssAntennaShortCircuit\":0,\"collisionRollover\":0,\"overspeedWarning\":0,\"cameraError\":0,\"illegalIgnition\":0,\"roadDrivingTimeout\":0,\"icModuleError\":0,\"banOnDrivingWarning\":0,\"stolen\":0,\"gnssModuleError\":0,\"cumulativeDrivingTimeout\":0,\"terminalMainPowerFailure\":0,\"driverFatigueMonitor\":0},\"msgId\":4610,\"encrypy\":0,\"vec2\":36,\"lon\":114.179001,\"vec1\":36,\"state\":{\"acc\":0,\"loadRating\":1,\"door\":0,\"laneDepartureWarning\":0,\"latLonEncryption\":0,\"electricCircuit\":0,\"location\":0,\"lon\":0,\"forwardCollisionWarning\":0,\"operation\":0,\"lat\":0,\"oilPath\":0},\"lat\":22.708228,\"direction\":343},\"DATATYPE\":4610,\"DATA\":\"AB4HB+kMEgQGzju5AVqABAAkACQAAF1eAVcAMABAAAMAAAAA\",\"UPEXGMSGREGISTER\":null,\"VEHICLENO\":\"粤BEJ546\",\"DATALEN\":36,\"VEHICLECOLOR\":2}";
        System.out.println("原文：" + str);
        SecretKeySpec secretKey = new SecretKeySpec(DatasecureUtils.keyBytes, "AES");
        System.out.println("使用的固定密钥: " + DatasecureUtils.bytesToHex(secretKey.getEncoded()));

        DatasecureUtils crypto = new DatasecureUtils(secretKey);
        byte[] encrypted = crypto.encrypt(str);
        System.out.println("\n加密结果 (IV+密文+标签):");
        System.out.println("长度: " + encrypted.length + " 字节");
        System.out.println("Hex: " + DatasecureUtils.bytesToHex(encrypted));

        String decrypted = crypto.decrypt(encrypted);
        System.out.println(decrypted);
    }
}
