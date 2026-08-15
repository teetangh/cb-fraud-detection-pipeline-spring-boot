package com.fraud.gateway.infra;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
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

    /**
     * Tolerance for clock drift between the minting service and this one.
     *
     * <p>Without it a healthy deployment 401s on minor NTP skew, which presents
     * as intermittent auth failures with no bad actor involved — the kind of
     * symptom that gets debugged for a day. 60s is far below the 300s token
     * lifetime, so it does not meaningfully extend the window.
     */
    private static final Duration CLOCK_SKEW = Duration.ofSeconds(60);

    /**
     * Static, so the constructor stays {@code (String secret)}. Jackson 3, the
     * same binding the rest of the service uses — the class javadoc's objection
     * was to dragging <em>Jackson 2</em> in via jjwt, not to parsing JSON.
     */
    private static final ObjectMapper MAPPER = new ObjectMapper();

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
            if (!MessageDigest.isEqual(expected, actual)) {
                return false;
            }

            // Signature first, claims second — never the reverse. Parsing claims
            // from an unverified token is reading attacker-controlled input and
            // acting on it.
            return notExpired(parts[1]);

        } catch (Exception e) {
            log.debug("JWT verification failed", e);
            return false;
        }
    }

    /**
     * Enforces the {@code exp} claim (issue #27).
     *
     * <p>Verifying the signature alone made the token lifetime decorative:
     * {@code HmacJwtMinter} stamps {@code exp = iat + 300}, and nothing read it,
     * so <b>a token that leaked once was valid forever</b>. The bounded lifetime
     * is the main thing limiting the blast radius of a captured token.
     *
     * <p>This is not the simplification spec §13 permits. That accepts a shared
     * secret instead of a real IdP (issue #6); an unenforced expiry is a separate
     * defect that would exist just the same with asymmetric keys.
     *
     * <p><b>A missing {@code exp} is a rejection, not a licence.</b> Treating
     * "no expiry claim" as "never expires" hands a forger the trivial bypass:
     * omit the field.
     */
    private boolean notExpired(String encodedClaims) {
        try {
            JsonNode claims = MAPPER.readTree(Base64.getUrlDecoder().decode(encodedClaims));
            JsonNode exp = claims.get("exp");
            if (exp == null || !exp.isNumber()) {
                log.debug("JWT rejected: no usable exp claim");
                return false;
            }
            Instant expiry = Instant.ofEpochSecond(exp.asLong());
            if (Instant.now().minus(CLOCK_SKEW).isBefore(expiry)) {
                return true;
            }
            log.debug("JWT rejected: expired at {}", expiry);
            return false;

        } catch (Exception e) {
            // Signature already verified, so this is a malformed token from a
            // holder of the secret, not an attack — still refused.
            log.debug("JWT rejected: claims not parseable", e);
            return false;
        }
    }

    private byte[] sign(String signingInput) throws Exception {
        Mac mac = Mac.getInstance(ALGORITHM);
        mac.init(new SecretKeySpec(secret, ALGORITHM));
        return mac.doFinal(signingInput.getBytes(StandardCharsets.UTF_8));
    }
}
