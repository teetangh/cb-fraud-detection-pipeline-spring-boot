package com.fraud.mock;

import com.fraud.mock.infra.HmacJwtMinter;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
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
 * compare. Asserting only "the minter produced three dot-separated parts" would
 * pass against a token no verifier accepts.
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
                    the token must carry a bounded lifetime. NOTE: gateway's HmacJwtVerifier \
                    verifies the signature and never reads exp, so this lifetime is not \
                    currently enforced anywhere — tracked as issue #27. This asserts the \
                    minter's half of the contract is sound and ready for that check to land.""")
                .isGreaterThan(iat);
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
            return MessageDigest.isEqual(expected, actual);
        } catch (Exception e) {
            return false;
        }
    }

    private long claimAsLong(String claims, String name) {
        int start = claims.indexOf("\"" + name + "\":") + name.length() + 3;
        int end = start;
        while (end < claims.length() && Character.isDigit(claims.charAt(end))) {
            end++;
        }
        return Long.parseLong(claims.substring(start, end));
    }
}
