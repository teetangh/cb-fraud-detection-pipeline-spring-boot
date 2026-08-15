package com.fraud.mock.infra;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;

/**
 * Mints the short-lived HMAC-SHA256 JWT the gateway verifies.
 *
 * <p>The fact that this class can exist at all IS the weakness of a shared
 * secret: the same key that verifies can also mint, so every verifier is
 * implicitly an issuer. Spec §13 accepts that for this build; asymmetric
 * signing is the fix and is tracked as issue #6.
 */
@Component
public class HmacJwtMinter {

    private static final String ALGORITHM = "HmacSHA256";
    private static final Base64.Encoder B64 = Base64.getUrlEncoder().withoutPadding();

    private final byte[] secret;

    public HmacJwtMinter(@Value("${fraud.jwt.secret}") String secret) {
        this.secret = secret.getBytes(StandardCharsets.UTF_8);
    }

    public String mint(String subject) {
        String header = B64.encodeToString(
                "{\"alg\":\"HS256\",\"typ\":\"JWT\"}".getBytes(StandardCharsets.UTF_8));
        long now = Instant.now().getEpochSecond();
        String payload = B64.encodeToString(
                ("{\"sub\":\"" + subject + "\",\"iat\":" + now + ",\"exp\":" + (now + 300) + "}")
                        .getBytes(StandardCharsets.UTF_8));
        String signingInput = header + "." + payload;
        return signingInput + "." + B64.encodeToString(sign(signingInput));
    }

    private byte[] sign(String signingInput) {
        try {
            Mac mac = Mac.getInstance(ALGORITHM);
            mac.init(new SecretKeySpec(secret, ALGORITHM));
            return mac.doFinal(signingInput.getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            throw new IllegalStateException("Could not sign JWT", e);
        }
    }
}
