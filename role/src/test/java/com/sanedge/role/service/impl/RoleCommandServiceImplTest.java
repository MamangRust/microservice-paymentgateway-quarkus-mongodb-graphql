package com.sanedge.role.service.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.function.Supplier;

import org.bson.types.ObjectId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.stubbing.Answer;

import com.sanedge.common.config.RedisService;
import com.sanedge.common.domain.response.ApiResponse;
import com.sanedge.common.exception.InvalidRequestException;
import com.sanedge.common.exception.ResourceAlreadyExistsException;
import com.sanedge.common.exception.ResourceNotFoundException;
import com.sanedge.common.observability.TracingMetrics;
import com.sanedge.role.domain.requests.CreateRoleRequest;
import com.sanedge.role.domain.requests.UpdateRoleRequest;
import com.sanedge.role.domain.response.RoleResponse;
import com.sanedge.role.domain.response.RoleResponseDeleteAt;
import com.sanedge.role.domain.response.UserRoleResponse;
import com.sanedge.role.entity.Role;
import com.sanedge.role.repository.RoleRepository;

import io.opentelemetry.api.common.Attributes;
import io.smallrye.mutiny.Uni;

@ExtendWith(MockitoExtension.class)
class RoleCommandServiceImplTest {

    private static final String ROLE_ID = "507f1f77bcf86cd799439001";
    private static final String ROLE_ID_2 = "507f1f77bcf86cd799439002";
    private static final String ROLE_ID_888 = "507f1f77bcf86cd799439888";
    private static final String MISSING_ID = "507f1f77bcf86cd799439099";
    private static final String USER_ID = "507f1f77bcf86cd799439011";
    private static final String USER_ID_999 = "507f1f77bcf86cd799439999";

    @Mock
    private RoleRepository roleRepository;

    @Mock
    private com.sanedge.role.repository.UserRoleRepository userRoleRepository;

    @Mock
    private RedisService redisService;

    @Mock
    private TracingMetrics tracingMetrics;

    private RoleCommandServiceImpl roleCommandService;

    @BeforeEach
    void setUp() {
        roleCommandService = new RoleCommandServiceImpl(
                roleRepository,
                userRoleRepository,
                redisService,
                tracingMetrics);

        lenient().doAnswer(invokeSupplier())
                .when(tracingMetrics).traceAndMeasure(anyString(), anyString(), any());
        lenient().doAnswer(invokeSupplier())
                .when(tracingMetrics)
                .traceAndMeasure(anyString(), anyString(), any(Attributes.class), any());
    }

    private Role createMockRole(String id, String roleName) {
        Role role = new Role();
        role.id = id != null ? new ObjectId(id) : null;
        role.setRoleName(roleName);
        role.setDeletedAt(null);
        role.setCreatedAt(Instant.now());
        role.setUpdatedAt(Instant.now());
        return role;
    }

    private com.sanedge.role.entity.UserRole createMockUserRole(String userId, String roleId) {
        com.sanedge.role.entity.UserRole userRole = new com.sanedge.role.entity.UserRole();
        userRole.setUserId(new ObjectId(userId));
        userRole.setRoleId(new ObjectId(roleId));
        return userRole;
    }

    @Test
    void createRole_roleAlreadyExists_throwsResourceAlreadyExistsException() {
        CreateRoleRequest request = new CreateRoleRequest();
        request.setName("Admin");

        Role existingRole = createMockRole(ROLE_ID, "Admin");

        when(roleRepository.findByRoleName("Admin")).thenReturn(Uni.createFrom().item(existingRole));

        try {
            roleCommandService.create(request).await().indefinitely();
            Assertions.fail("Expected ResourceAlreadyExistsException");
        } catch (ResourceAlreadyExistsException e) {
            assertThat(e.getMessage()).contains("already exists");
        }
    }

    @Test
    void createRole_success_createsNewRole() {
        CreateRoleRequest request = new CreateRoleRequest();
        request.setName("NewRole");

        lenient().when(roleRepository.findByRoleName("NewRole")).thenReturn(Uni.createFrom().nullItem());
        lenient().when(roleRepository.persist(any(Role.class))).thenAnswer(invocation -> {
            Role roleToPersist = invocation.getArgument(0);
            roleToPersist.id = new ObjectId(ROLE_ID);
            return Uni.createFrom().item(roleToPersist);
        });
        lenient().when(redisService.deleteReactive(anyString())).thenReturn(Uni.createFrom().voidItem());

        ApiResponse<RoleResponse> response = roleCommandService.create(request).await().indefinitely();

        assertThat(response.status()).isEqualTo("success");
        assertThat(response.message()).isEqualTo("Role created successfully");
        assertThat(response.data()).isNotNull();
        assertThat(response.data().getName()).isEqualTo("NewRole");
    }

    @Test
    void updateRole_roleNotFound_throwsResourceNotFoundException() {
        UpdateRoleRequest request = new UpdateRoleRequest();
        request.setRoleId(MISSING_ID);
        request.setName("UpdatedRole");

        when(roleRepository.findById(anyString())).thenReturn(Uni.createFrom().nullItem());

        try {
            roleCommandService.update(request).await().indefinitely();
            Assertions.fail("Expected ResourceNotFoundException");
        } catch (ResourceNotFoundException e) {
            assertThat(e.getMessage()).contains("Role not found with id: " + request.getRoleId());
        }
    }

    @Test
    void updateRole_nameAlreadyExists_throwsResourceAlreadyExistsException() {
        Role existingRole = createMockRole(ROLE_ID, "OldRoleName");

        UpdateRoleRequest request = new UpdateRoleRequest();
        request.setRoleId(ROLE_ID);
        request.setName("AnotherAdmin");

        Role conflictingRole = createMockRole(ROLE_ID_2, "AnotherAdmin");

        when(roleRepository.findById(anyString())).thenReturn(Uni.createFrom().item(existingRole));
        lenient().when(roleRepository.findByRoleName("AnotherAdmin"))
                .thenReturn(Uni.createFrom().item(conflictingRole));

        try {
            roleCommandService.update(request).await().indefinitely();
            Assertions.fail("Expected ResourceAlreadyExistsException");
        } catch (ResourceAlreadyExistsException e) {
            assertThat(e.getMessage()).contains("already exists");
        }
    }

    @Test
    void updateRole_success_updatesRolename() {
        Role existingRole = createMockRole(ROLE_ID, "OldRoleName");

        UpdateRoleRequest request = new UpdateRoleRequest();
        request.setRoleId(ROLE_ID);
        request.setName("NewRoleName");

        when(roleRepository.findById(anyString())).thenReturn(Uni.createFrom().item(existingRole));
        lenient().when(roleRepository.findByRoleName("NewRoleName")).thenReturn(Uni.createFrom().nullItem());
        lenient().when(roleRepository.persist(any(Role.class))).thenReturn(Uni.createFrom().item(existingRole));
        when(redisService.deleteReactive(anyString())).thenReturn(Uni.createFrom().voidItem());

        ApiResponse<RoleResponse> response = roleCommandService.update(request).await().indefinitely();

        assertThat(response.status()).isEqualTo("success");
        assertThat(response.message()).isEqualTo("Role updated successfully");
        assertThat(response.data().getName()).isEqualTo("NewRoleName");
    }

    @Test
    void updateRole_sameName_noOp_noException() {
        Role existingRole = createMockRole(ROLE_ID, "SameRoleName");

        UpdateRoleRequest request = new UpdateRoleRequest();
        request.setRoleId(ROLE_ID);
        request.setName("SameRoleName");

        when(roleRepository.findById(anyString())).thenReturn(Uni.createFrom().item(existingRole));

        ApiResponse<RoleResponse> response = roleCommandService.update(request).await().indefinitely();

        assertThat(response.status()).isEqualTo("success");
        assertThat(response.message()).isEqualTo("Role updated successfully");
        verify(roleRepository, never()).findByRoleName(anyString());
        verify(roleRepository, never()).persist(any(Role.class));
    }

    @Test
    void trashRole_roleNotFound_throwsResourceNotFoundException() {
        String roleId = MISSING_ID;

        when(roleRepository.trash(roleId)).thenReturn(Uni.createFrom().nullItem());

        try {
            roleCommandService.trash(roleId).await().indefinitely();
            Assertions.fail("Expected ResourceNotFoundException");
        } catch (ResourceNotFoundException e) {
            assertThat(e.getMessage()).contains("Role not found with id: " + roleId);
        }
    }

    @Test
    void trashRole_success_trashRole() {
        Role trashedRole = createMockRole(ROLE_ID, "TrashedRole");
        trashedRole.setDeletedAt(Instant.now());

        when(roleRepository.trash(anyString())).thenReturn(Uni.createFrom().item(trashedRole));
        when(redisService.deleteReactive(anyString())).thenReturn(Uni.createFrom().voidItem());

        ApiResponse<RoleResponseDeleteAt> response = roleCommandService.trash(ROLE_ID).await().indefinitely();

        assertThat(response.status()).isEqualTo("success");
        assertThat(response.message()).isEqualTo("Role trashed successfully");
        assertThat(response.data()).isNotNull();
        assertThat(response.data().getName()).isEqualTo("TrashedRole");
    }

    @Test
    void restoreRole_roleNotTrashed_throwsResourceNotFoundException() {
        String roleId = ROLE_ID;

        when(roleRepository.restore(roleId)).thenReturn(Uni.createFrom().nullItem());

        try {
            roleCommandService.restore(roleId).await().indefinitely();
            Assertions.fail("Expected InvalidRequestException");
        } catch (InvalidRequestException e) {
            assertThat(e.getMessage()).contains("must be trashed first");
        }
    }

    @Test
    void restoreRole_success_restoreRole() {
        Role restoredRole = createMockRole(ROLE_ID, "RestoredRole");

        when(roleRepository.restore(anyString())).thenReturn(Uni.createFrom().item(restoredRole));
        when(redisService.deleteReactive(anyString())).thenReturn(Uni.createFrom().voidItem());

        ApiResponse<RoleResponseDeleteAt> response = roleCommandService.restore(ROLE_ID).await().indefinitely();

        assertThat(response.status()).isEqualTo("success");
        assertThat(response.message()).isEqualTo("Role restored successfully");
        assertThat(response.data()).isNotNull();
        assertThat(response.data().getName()).isEqualTo("RestoredRole");
    }

    @Test
    void deletePermanent_roleNotFoundOrNotTrashed_throwsInvalidRequestException() {
        String roleId = MISSING_ID;

        when(roleRepository.deletePermanent(roleId)).thenReturn(Uni.createFrom().nullItem());

        try {
            roleCommandService.deletePermanent(roleId).await().indefinitely();
            Assertions.fail("Expected InvalidRequestException");
        } catch (InvalidRequestException e) {
            assertThat(e.getMessage()).contains("must be trashed");
        }
    }

    @Test
    void deletePermanent_success_deleteRole() {
        Role deletedRole = createMockRole(ROLE_ID, "DeletedRole");
        deletedRole.setDeletedAt(Instant.now());

        when(roleRepository.deletePermanent(anyString())).thenReturn(Uni.createFrom().item(deletedRole));
        when(redisService.deleteReactive(anyString())).thenReturn(Uni.createFrom().voidItem());

        ApiResponse<Void> response = roleCommandService.deletePermanent(ROLE_ID).await().indefinitely();

        assertThat(response.status()).isEqualTo("success");
        assertThat(response.message()).isEqualTo("Role deleted permanently");
    }

    @Test
    void restoreAllTrashedRoles_noTrashedRoles_throwsResourceNotFoundException() {
        when(roleRepository.restoreAllDeleted()).thenReturn(Uni.createFrom().item(false));

        try {
            roleCommandService.restoreAllTrashedRoles().await().indefinitely();
            Assertions.fail("Expected ResourceNotFoundException");
        } catch (ResourceNotFoundException e) {
            assertThat(e.getMessage()).contains("No trashed roles found");
        }
    }

    @Test
    void restoreAllTrashedRoles_success_restoreAll() {
        when(roleRepository.restoreAllDeleted()).thenReturn(Uni.createFrom().item(true));
        lenient().when(redisService.deleteReactive(anyString())).thenReturn(Uni.createFrom().voidItem());

        ApiResponse<Void> response = roleCommandService.restoreAllTrashedRoles().await().indefinitely();

        assertThat(response.status()).isEqualTo("success");
        assertThat(response.message()).contains("have been restored");
    }

    @Test
    void deleteAllTrashedRoles_noTrashedRoles_throwsResourceNotFoundException() {
        when(roleRepository.deleteAllDeleted()).thenReturn(Uni.createFrom().item(false));

        try {
            roleCommandService.deleteAllTrashedRoles().await().indefinitely();
            Assertions.fail("Expected ResourceNotFoundException");
        } catch (ResourceNotFoundException e) {
            assertThat(e.getMessage()).contains("No trashed roles found");
        }
    }

    @Test
    void deleteAllTrashedRoles_success_deleteAll() {
        when(roleRepository.deleteAllDeleted()).thenReturn(Uni.createFrom().item(true));
        lenient().when(redisService.deleteReactive(anyString())).thenReturn(Uni.createFrom().voidItem());

        ApiResponse<Void> response = roleCommandService.deleteAllTrashedRoles().await().indefinitely();

        assertThat(response.status()).isEqualTo("success");
        assertThat(response.message()).contains("deleted permanently");
    }

    @Test
    void assignRoleToUser_success_assignsRole() {
        com.sanedge.role.entity.UserRole userRole = createMockUserRole(USER_ID, ROLE_ID_2);

        when(userRoleRepository.assignRole(anyString(), anyString()))
                .thenReturn(Uni.createFrom().item(userRole));

        ApiResponse<UserRoleResponse> response = roleCommandService.assignRoleToUser(USER_ID, ROLE_ID_2)
                .await().indefinitely();

        assertThat(response.status()).isEqualTo("success");
        assertThat(response.message()).isEqualTo("Role assigned to user successfully");
        assertThat(response.data()).isNotNull();
    }

    @Test
    void removeRoleFromUser_notFound_throwsResourceNotFoundException() {
        String userId = USER_ID_999;
        String roleId = ROLE_ID_888;

        when(userRoleRepository.removeRole(userId, roleId)).thenReturn(Uni.createFrom().item(false));

        try {
            roleCommandService.removeRoleFromUser(userId, roleId).await().indefinitely();
            Assertions.fail("Expected ResourceNotFoundException");
        } catch (ResourceNotFoundException e) {
            assertThat(e.getMessage()).contains("UserRole association not found");
        }
    }

    @Test
    void removeRoleFromUser_success_removesRole() {
        when(userRoleRepository.removeRole(anyString(), anyString())).thenReturn(Uni.createFrom().item(true));
        lenient().when(redisService.deleteReactive(anyString())).thenReturn(Uni.createFrom().voidItem());

        ApiResponse<Void> response = roleCommandService.removeRoleFromUser(USER_ID, ROLE_ID_2).await().indefinitely();

        assertThat(response.status()).isEqualTo("success");
        assertThat(response.message()).isEqualTo("Role removed from user successfully");
    }

    private Answer<Uni<?>> invokeSupplier() {
        return invocation -> {
            Supplier<?> supplier = null;
            for (Object arg : invocation.getArguments()) {
                if (arg instanceof Supplier<?>) {
                    supplier = (Supplier<?>) arg;
                    break;
                }
            }
            return supplier != null ? (Uni<?>) supplier.get() : null;
        };
    }
}
