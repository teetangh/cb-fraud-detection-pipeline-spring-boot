package com.fraud.gateway;

import com.fraud.gateway.infra.HmacJwtVerifier;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pure unit test, no containers.
 *
 * <p>Auth is the one place where a test that only checks the happy path is
 * actively harmful: "a valid token is accepted" passes just as well on a
 * verifier that accepts everything. Every case below is a rejection.
 */
@DisplayName("HMAC JWT verification")
class HmacJwtVerifierTest {

    private static final String SECRET = "test-secret-value-for-unit-tests";
    private final HmacJwtVerifier verifier = new HmacJwtVerifier(SECRET);

    @Test
    @DisplayName("accepts a token signed with the shared secret")
    void acceptsValidToken() {
        assertThat(verifier.verify("Bearer " + mint(SECRET))).isTrue();
    }

    @Test
    @DisplayName("accepts a bare token without the Bearer prefix")
    void acceptsBareToken() {
        assertThat(verifier.verify(mint(SECRET))).isTrue();
    }

    @Test
    @DisplayName("REJECTS a token signed with a different secret")
    void rejectsWrongSecret() {
        // The core property. If this passes, the shared secret means nothing.
        assertThat(verifier.verify("Bearer " + mint("a-completely-different-secret"))).isFalse();
    }

    @Test
    @DisplayName("REJECTS a token whose payload was tampered with after signing")
    void rejectsTamperedPayload() {
        String token = mint(SECRET);
        String[] parts = token.split("\\.");
        String forgedPayload = Base64.getUrlEncoder().withoutPadding().encodeToString(
                "{\"sub\":\"attacker\",\"iat\":0,\"exp\":9999999999}".getBytes(StandardCharsets.UTF_8));

        // Signature kept, payload swapped — the classic forgery attempt.
        assertThat(verifier.verify(parts[0] + "." + forgedPayload + "." + parts[2])).isFalse();
    }

    @Test
    @DisplayName("REJECTS structurally invalid input without throwing")
    void rejectsMalformed() {
        assertThat(verifier.verify(null)).isFalse();
        assertThat(verifier.verify("")).isFalse();
        assertThat(verifier.verify("Bearer ")).isFalse();
        assertThat(verifier.verify("Bearer not-a-jwt")).isFalse();
        assertThat(verifier.verify("Bearer only.two")).isFalse();
        assertThat(verifier.verify("Bearer a.b.c.d")).isFalse();
        // A malformed token must be a clean false, never a 500 — otherwise
        // garbage input becomes a denial-of-service surface.
        assertThat(verifier.verify("Bearer !!!.@@@.###")).isFalse();
    }

    @Test
    @DisplayName("REJECTS the alg=none unsigned-token attack")
    void rejectsEmptySignature() {
        String token = mint(SECRET);
        String[] parts = token.split("\\.");
        assertThat(verifier.verify(parts[0] + "." + parts[1] + ".")).isFalse();
    }

    // ── exp enforcement (issue #27) ──────────────────────────────────────────

    @Test
    @DisplayName("REJECTS an expired token, even though the signature is valid")
    void rejectsExpiredToken() {
        long now = Instant.now().getEpochSecond();
        // Signed with the RIGHT secret. Only the expiry is wrong — so this can
        // only pass if exp is actually being read.
        String expired = mintWithClaims(SECRET,
                "{\"sub\":\"mock-payment-api\",\"iat\":" + (now - 7200)
                + ",\"exp\":" + (now - 3600) + "}");

        assertThat(verifier.verify("Bearer " + expired))
                .as("""
                    a token that leaked once must not be valid forever. The signature \
                    check alone made the 300s lifetime decorative — this is the \
                    assertion that keeps it real (issue #27).""")
                .isFalse();
    }

    @Test
    @DisplayName("REJECTS a validly-signed token with NO exp claim")
    void rejectsTokenWithoutExpiry() {
        String noExpiry = mintWithClaims(SECRET, "{\"sub\":\"mock-payment-api\"}");

        assertThat(verifier.verify("Bearer " + noExpiry))
                .as("""
                    absent exp must be a rejection, not a licence. Treating "no expiry \
                    claim" as "never expires" hands anyone able to mint the trivial \
                    bypass: omit the field.""")
                .isFalse();
    }

    @Test
    @DisplayName("REJECTS a non-numeric exp rather than throwing")
    void rejectsNonNumericExpiry() {
        String weird = mintWithClaims(SECRET,
                "{\"sub\":\"x\",\"exp\":\"not-a-number\"}");

        assertThat(verifier.verify("Bearer " + weird))
                .as("a malformed claim must be a clean false, never a 500 — otherwise it "
                    + "becomes a denial-of-service surface")
                .isFalse();
    }

    @Test
    @DisplayName("accepts a token that expired within the clock-skew tolerance")
    void acceptsWithinClockSkew() {
        long now = Instant.now().getEpochSecond();
        String justExpired = mintWithClaims(SECRET,
                "{\"sub\":\"x\",\"iat\":" + (now - 310) + ",\"exp\":" + (now - 10) + "}");

        assertThat(verifier.verify("Bearer " + justExpired))
                .as("""
                    10s past expiry is inside the 60s skew tolerance. Without it, minor \
                    NTP drift between services presents as intermittent 401s with no bad \
                    actor involved — a day of debugging for no security gain.""")
                .isTrue();
    }

    @Test
    @DisplayName("REJECTS a token whose exp was extended without re-signing")
    void rejectsExtendedExpiryWithoutResigning() {
        long now = Instant.now().getEpochSecond();
        String expired = mintWithClaims(SECRET,
                "{\"sub\":\"x\",\"iat\":" + (now - 7200) + ",\"exp\":" + (now - 3600) + "}");
        String[] parts = expired.split("\\.");

        // The obvious attack once exp is enforced: keep the valid signature,
        // swap in a far-future expiry.
        String extended = Base64.getUrlEncoder().withoutPadding().encodeToString(
                ("{\"sub\":\"x\",\"iat\":0,\"exp\":9999999999}").getBytes(StandardCharsets.UTF_8));

        assertThat(verifier.verify(parts[0] + "." + extended + "." + parts[2]))
                .as("exp is inside the signed payload, so extending it must invalidate "
                    + "the signature — the check order (signature THEN claims) is what "
                    + "makes that hold")
                .isFalse();
    }

    private static String mintWithClaims(String secret, String claimsJson) {
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

    private static String mint(String secret) {
        try {
            var b64 = Base64.getUrlEncoder().withoutPadding();
            String header = b64.encodeToString("{\"alg\":\"HS256\",\"typ\":\"JWT\"}".getBytes(StandardCharsets.UTF_8));
            long now = Instant.now().getEpochSecond();
            String payload = b64.encodeToString(
                    ("{\"sub\":\"mock-payment-api\",\"iat\":" + now + ",\"exp\":" + (now + 300) + "}")
                            .getBytes(StandardCharsets.UTF_8));
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
