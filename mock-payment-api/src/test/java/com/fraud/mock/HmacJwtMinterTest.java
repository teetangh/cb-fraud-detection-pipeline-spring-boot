package com.fraud.mock;

import com.fraud.mock.infra.HmacJwtMinter;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #14 — the minted token is one gateway-service will actually accept.
 *
 * <p>This is a <b>cross-service contract test without a shared jar</b>. Spec §3
 * forbids a shared DTO/util module, so {@code HmacJwtMinter} (here) and
 * {@code HmacJwtVerifier} (in gateway-service) are two independent
 * implementations of one wire format, with nothing but documentation holding
 * them together. If they drift, every request 401s — and the two services build
 * separately, so nothing else in the repo would catch it.
 *
 * <p>So these tests re-implement the verifier's check exactly as
 * {@code HmacJwtVerifier} performs it — split on {@code .}, HMAC-SHA256 over
 * {@code header + "." + payload}, url-safe Base64 without padding, constant-time
 * compare, then the {@code exp} check. Asserting only "the minter produced three
 * dot-separated parts" would pass against a token no verifier accepts.
 *
 * <p><b>This mirror has to be kept in step with the verifier.</b> Issue #27 added
 * {@code exp} enforcement to the gateway; until that was mirrored here, this class
 * still passed while claiming to prove something it no longer checked — a minter
 * that dropped {@code exp} would have been green here and 401 in production.
 */
@DisplayName("Issue #14 — minted JWT satisfies gateway's verification contract")
class HmacJwtMinterTest {

    private static final String SECRET = "test-secret-that-is-long-enough-for-hmac-sha256";

    @Test
    @DisplayName("a token minted with the shared secret verifies under gateway's algorithm")
    void mintedTokenVerifies() {
        String token = new HmacJwtMinter(SECRET).mint("mock-payment-api");

        assertThat(verifyAsGatewayWould(token, SECRET))
                .as("""
                    the minter and gateway's HmacJwtVerifier are independent implementations \
                    of one wire format (no shared jar, spec §3). If this fails they have \
                    drifted and every request 401s.""")
                .isTrue();
    }

    @Test
    @DisplayName("a token minted with a DIFFERENT secret is rejected")
    void wrongSecretIsRejected() {
        String forged = new HmacJwtMinter("a-completely-different-secret-value").mint("attacker");

        assertThat(verifyAsGatewayWould(forged, SECRET))
                .as("""
                    without this the previous test proves nothing: a verifier that returns \
                    true unconditionally would also pass it. This is the case that has to \
                    fail for the signature check to mean anything.""")
                .isFalse();
    }

    @Test
    @DisplayName("tampering with the payload invalidates the signature")
    void tamperedPayloadIsRejected() {
        String token = new HmacJwtMinter(SECRET).mint("mock-payment-api");
        String[] parts = token.split("\\.");

        String tamperedPayload = Base64.getUrlEncoder().withoutPadding().encodeToString(
                "{\"sub\":\"admin\",\"iat\":0,\"exp\":9999999999}".getBytes(StandardCharsets.UTF_8));
        String tampered = parts[0] + "." + tamperedPayload + "." + parts[2];

        assertThat(verifyAsGatewayWould(tampered, SECRET))
                .as("the signature covers header.payload, so swapping the claims must "
                    + "invalidate it — otherwise any caller could self-promote their subject")
                .isFalse();
    }

    @Test
    @DisplayName("the token carries the subject and a bounded lifetime")
    void tokenCarriesSubjectAndExpiry() {
        String token = new HmacJwtMinter(SECRET).mint("mock-payment-api");
        String claims = new String(Base64.getUrlDecoder().decode(token.split("\\.")[1]),
                                   StandardCharsets.UTF_8);

        assertThat(claims).contains("\"sub\":\"mock-payment-api\"");

        long iat = claimAsLong(claims, "iat");
        long exp = claimAsLong(claims, "exp");
        assertThat(exp)
                .as("""
                    the token must carry a bounded lifetime. gateway's HmacJwtVerifier \
                    enforces this claim as of issue #27, so a minter that stopped emitting \
                    exp would 401 every request — that is what makes this assertion \
                    load-bearing rather than descriptive.""")
                .isGreaterThan(iat);
    }

    @Test
    @DisplayName("a validly-signed token with NO exp is rejected — the drift this class exists to catch")
    void tokenWithoutExpiryIsRejected() {
        // Not something HmacJwtMinter produces today. That is the point: this is
        // the regression that would otherwise reach production silently, because
        // the two services build separately and the minter's own tests would all
        // still pass. Since #27 the gateway rejects it, so the contract test has
        // to reject it too, or it stops mirroring the verifier.
        String noExpiry = signAsMinterWould(SECRET, "{\"sub\":\"mock-payment-api\",\"iat\":0}");

        assertThat(verifyAsGatewayWould(noExpiry, SECRET))
                .as("""
                    absent exp is a rejection at the gateway, not a licence. If this class \
                    kept mirroring the pre-#27 verifier, a minter regression that dropped \
                    exp would leave every test here green while every real request 401s.""")
                .isFalse();
    }

    @Test
    @DisplayName("a validly-signed but EXPIRED token is rejected")
    void expiredTokenIsRejected() {
        long now = Instant.now().getEpochSecond();
        String expired = signAsMinterWould(SECRET,
                "{\"sub\":\"mock-payment-api\",\"iat\":" + (now - 7200)
                + ",\"exp\":" + (now - 3600) + "}");

        assertThat(verifyAsGatewayWould(expired, SECRET))
                .as("signed with the right secret — only the lifetime is spent, so this can "
                    + "only pass if the mirrored verifier actually reads exp")
                .isFalse();
    }

    // ── gateway-service's HmacJwtVerifier.verify(), re-implemented ────────────

    private boolean verifyAsGatewayWould(String bearerToken, String secret) {
        if (bearerToken == null || bearerToken.isBlank()) {
            return false;
        }
        String token = bearerToken.startsWith("Bearer ") ? bearerToken.substring(7) : bearerToken;
        String[] parts = token.split("\\.");
        if (parts.length != 3) {
            return false;
        }
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
            byte[] expected = mac.doFinal(
                    (parts[0] + "." + parts[1]).getBytes(StandardCharsets.UTF_8));
            byte[] actual = Base64.getUrlDecoder().decode(parts[2]);
            if (!MessageDigest.isEqual(expected, actual)) {
                return false;
            }
            // Signature first, claims second — the same order the gateway uses.
            // Parsing claims out of an unverified token would be acting on
            // attacker-controlled input.
            return notExpiredAsGatewayWould(parts[1]);
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * The verifier's {@code exp} enforcement (issue #27), mirrored: absent or
     * non-numeric {@code exp} is a rejection, and expiry is allowed 60s of
     * clock-skew tolerance.
     *
     * <p>Deliberately a second, independent implementation — string scraping
     * here against Jackson there. A shared helper would defeat the purpose of
     * the class: two implementations that agree by construction cannot detect
     * that they have drifted.
     */
    private boolean notExpiredAsGatewayWould(String encodedClaims) {
        String claims = new String(Base64.getUrlDecoder().decode(encodedClaims),
                                   StandardCharsets.UTF_8);
        Long exp = optionalClaimAsLong(claims, "exp");
        if (exp == null) {
            return false;
        }
        return Instant.now().minusSeconds(60).isBefore(Instant.ofEpochSecond(exp));
    }

    private long claimAsLong(String claims, String name) {
        Long value = optionalClaimAsLong(claims, name);
        if (value == null) {
            throw new AssertionError("claim absent or not a bare number: " + name);
        }
        return value;
    }

    /** @return the claim as a long, or null when absent or not a bare number. */
    private Long optionalClaimAsLong(String claims, String name) {
        String key = "\"" + name + "\":";
        int at = claims.indexOf(key);
        if (at < 0) {
            return null;
        }
        int start = at + key.length();
        int end = start;
        while (end < claims.length() && Character.isDigit(claims.charAt(end))) {
            end++;
        }
        return end == start ? null : Long.parseLong(claims.substring(start, end));
    }

    /**
     * Mints with arbitrary claims, using the same wire format as
     * {@link HmacJwtMinter}. Needed because the negative cases above are tokens
     * the real minter deliberately cannot produce.
     */
    private static String signAsMinterWould(String secret, String claimsJson) {
        try {
            var b64 = Base64.getUrlEncoder().withoutPadding();
            String header = b64.encodeToString(
                    "{\"alg\":\"HS256\",\"typ\":\"JWT\"}".getBytes(StandardCharsets.UTF_8));
            String payload = b64.encodeToString(claimsJson.getBytes(StandardCharsets.UTF_8));
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
            String signingInput = header + "." + payload;
            return signingInput + "." + b64.encodeToString(
                    mac.doFinal(signingInput.getBytes(StandardCharsets.UTF_8)));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
