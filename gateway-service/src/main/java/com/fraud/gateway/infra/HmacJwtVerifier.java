package com.fraud.gateway.infra;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Base64;

/**
 * Shared-secret HMAC-SHA256 JWT verification.
 *
 * <p>Spec §13 explicitly accepts a shared-secret check as sufficient for this
 * build; a real IdP with JWKS is tracked as issue #6. The weakness that
 * simplification implies is worth naming: with a symmetric secret, **every
 * verifier can also mint**. Asymmetric signing is the fix, not more code here.
 *
 * <p>Hand-rolled rather than pulling in jjwt, whose Jackson binding would drag
 * Jackson 2 into a Jackson 3 build for what is essentially one
 * {@code Mac.doFinal}.
 */
@Component
public class HmacJwtVerifier {

    private static final Logger log = LoggerFactory.getLogger(HmacJwtVerifier.class);
    private static final String ALGORITHM = "HmacSHA256";

    private final byte[] secret;

    public HmacJwtVerifier(@Value("${fraud.jwt.secret}") String secret) {
        this.secret = secret.getBytes(StandardCharsets.UTF_8);
    }

    /** @return the subject claim if the token verifies, otherwise empty. */
    public boolean verify(String bearerToken) {
        if (bearerToken == null || bearerToken.isBlank()) {
            return false;
        }
        String token = bearerToken.startsWith("Bearer ") ? bearerToken.substring(7) : bearerToken;

        String[] parts = token.split("\\.");
        if (parts.length != 3) {
            return false;
        }

        try {
            byte[] expected = sign(parts[0] + "." + parts[1]);
            byte[] actual = Base64.getUrlDecoder().decode(parts[2]);

            // MessageDigest.isEqual is CONSTANT-TIME. A String.equals or
            // Arrays.equals on a MAC leaks, through timing, how many leading
            // bytes matched — which is enough to forge a signature byte by byte.
            // This is the one line in the class that must not be "simplified".
            return MessageDigest.isEqual(expected, actual);

        } catch (Exception e) {
            log.debug("JWT verification failed", e);
            return false;
        }
    }

    private byte[] sign(String signingInput) throws Exception {
        Mac mac = Mac.getInstance(ALGORITHM);
        mac.init(new SecretKeySpec(secret, ALGORITHM));
        return mac.doFinal(signingInput.getBytes(StandardCharsets.UTF_8));
    }
}
