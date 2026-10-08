package com.sanedge.role.service.impl;

import java.time.Instant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sanedge.common.config.RedisService;
import com.sanedge.role.domain.requests.CreateRoleRequest;
import com.sanedge.role.domain.requests.UpdateRoleRequest;
import com.sanedge.common.domain.response.ApiResponse;
import com.sanedge.role.domain.response.RoleResponse;
import com.sanedge.role.domain.response.RoleResponseDeleteAt;
import com.sanedge.role.domain.response.UserRoleResponse;
import com.sanedge.role.entity.Role;
import com.sanedge.common.exception.ResourceAlreadyExistsException;
import com.sanedge.common.exception.ResourceNotFoundException;
import com.sanedge.common.exception.InvalidRequestException;
import com.sanedge.common.observability.TracingMetrics;
import com.sanedge.role.repository.RoleRepository;
import com.sanedge.role.repository.UserRoleRepository;
import com.sanedge.role.service.RoleCommandService;

import io.opentelemetry.api.common.Attributes;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class RoleCommandServiceImpl implements RoleCommandService {
        private static final Logger logger = LoggerFactory.getLogger(RoleCommandServiceImpl.class);

        RoleRepository roleRepository;
        UserRoleRepository userRoleRepository;
        RedisService redisService;
        TracingMetrics tracingMetrics;

        @Inject
        public RoleCommandServiceImpl(RoleRepository roleRepository, UserRoleRepository userRoleRepository,
                        RedisService redisService, TracingMetrics tracingMetrics) {
                this.roleRepository = roleRepository;
                this.userRoleRepository = userRoleRepository;
                this.redisService = redisService;
                this.tracingMetrics = tracingMetrics;
        }

        @Override
        public Uni<ApiResponse<RoleResponse>> create(CreateRoleRequest request) {
                logger.info("Creating new role with name: {}", request.getName());

                Attributes attrs = Attributes.builder()
                                .put("operation", "create_role")
                                .put("service.name", "role-service")
                                .put("role.name", request.getName())
                                .build();

                return tracingMetrics.<ApiResponse<RoleResponse>>traceAndMeasure(
                                "createRole", "create_role", attrs, () -> {

                                        return roleRepository.findByRoleName(request.getName())
                                                        .chain(existingRole -> {
                                                                 if (existingRole != null) {
                                                                         logger.warn("Role creation failed - role already exists: {}",
                                                                                         request.getName());
                                                                         throw new ResourceAlreadyExistsException(
                                                                                         "Role with name '" + request.getName()
                                                                                                         + "' already exists");
                                                                 }

                                                                 Role newRole = new Role();
                                                                 newRole.setRoleName(request.getName());
                                                                 Instant now = Instant.now();
                                                                 newRole.setCreatedAt(now);
                                                                 newRole.setUpdatedAt(now);
                                                                 return roleRepository.persist(newRole)
                                                                                 .map(v -> {
                                                                                         RoleResponse roleResponse = RoleResponse
                                                                                                         .from(newRole);

                                                                                         logger.info("Successfully created role with id: {} and name: {}",
                                                                                                         newRole.id,
                                                                                                         newRole.getRoleName());
                                                                                         logger.info("Role created. List caches will be refreshed upon expiry.");

                                                                                         return ApiResponse.success(
                                                                                                         "Role created successfully",
                                                                                                         roleResponse);
                                                                                 });
                                                         });
                                }).onFailure().invoke(e -> {
                                        logger.error("Error creating role with name: {}", request.getName(), e);
                                });
        }

        @Override
        public Uni<ApiResponse<RoleResponse>> update(UpdateRoleRequest request) {
                logger.info("Updating role with id: {} to new name: {}", request.getRoleId(), request.getName());

                Attributes attrs = Attributes.builder()
                                .put("operation", "update_role")
                                .put("service.name", "role-service")
                                .put("role.id", String.valueOf(request.getRoleId()))
                                .build();

                return tracingMetrics.<ApiResponse<RoleResponse>>traceAndMeasure(
                                "updateRole", "update_role", attrs, () -> {

                                        return roleRepository.findById(request.getRoleId())
                                                        .chain(existingRole -> {
                                                                 if (existingRole == null) {
                                                                         logger.warn("Role update failed - role not found with id: {}",
                                                                                         request.getRoleId());
                                                                         throw new ResourceNotFoundException(
                                                                                         "Role not found with id: " + request.getRoleId());
                                                                 }

                                                                 // No-op: name unchanged, skip update and cache invalidation
                                                                 if (existingRole.getRoleName().equals(request.getName())) {
                                                                         RoleResponse roleResponse = RoleResponse
                                                                                         .from(existingRole);
                                                                         return Uni.createFrom().item(
                                                                                         ApiResponse.success("Role updated successfully",
                                                                                                         roleResponse));
                                                                 }

                                                                 return roleRepository.findByRoleName(request.getName())
                                                                                 .chain(duplicateRole -> {
                                                                                         if (duplicateRole != null) {
                                                                                                 logger.warn(
                                                                                                                 "Role update failed - new name '{}' already exists for another role",
                                                                                                                 request.getName());
                                                                                                 throw new ResourceAlreadyExistsException(
                                                                                                                 "Role with name '"
                                                                                                                                 + request.getName()
                                                                                                                                 + "' already exists");
                                                                                         }

                                                                                         existingRole.setRoleName(
                                                                                                         request.getName());
                                                                                         existingRole.setUpdatedAt(
                                                                                                         Instant.now());
                                                                                         return roleRepository
                                                                                                         .persist(existingRole)
                                                                                                         .chain(updatedRole -> {
                                                                                                                 RoleResponse roleResponse = RoleResponse
                                                                                                                                 .from(updatedRole);
                                                                                                                 String cacheKey = "role:"
                                                                                                                                 + request.getRoleId();
                                                                                                                 return redisService
                                                                                                                                 .deleteReactive(cacheKey)
                                                                                                                                 .map(v -> {
                                                                                                                                         logger.info("Invalidated cache for key: {}",
                                                                                                                                                         cacheKey);
                                                                                                                                         logger.info("Successfully updated role with id: {}",
                                                                                                                                                         request.getRoleId());
                                                                                                                                         return ApiResponse.success(
                                                                                                                                                         "Role updated successfully",
                                                                                                                                                         roleResponse);
                                                                                                                                 });
                                                                                                         });
                                                                                 });
                                                         });
                                }).onFailure().invoke(e -> {
                                        logger.error("Error updating role with id: {}", request.getRoleId(), e);
                                });
        }

        @Override
        public Uni<ApiResponse<RoleResponseDeleteAt>> trash(String id) {
                logger.info("Trashing role with id: {}", id);

                Attributes attrs = Attributes.builder()
                                .put("operation", "trash_role")
                                .put("service.name", "role-service")
                                .put("role.id", String.valueOf(id))
                                .build();

                return tracingMetrics.<ApiResponse<RoleResponseDeleteAt>>traceAndMeasure(
                                "trashRole", "trash_role", attrs, () -> {

                                        return roleRepository.trash(id)
                                                        .chain(trashedRole -> {
                                                                 if (trashedRole == null) {
                                                                         logger.warn(
                                                                                         "Role trash failed - role not found with id: {}",
                                                                                         id);
                                                                         throw new ResourceNotFoundException(
                                                                                         "Role not found with id: " + id);
                                                                 }

                                                                 RoleResponseDeleteAt roleResponseDeleteAt = RoleResponseDeleteAt
                                                                                 .from(trashedRole);
                                                                 String cacheKey = "role:" + id;

                                                                 return redisService.deleteReactive(cacheKey)
                                                                                 .map(v -> {
                                                                                         logger.info("Invalidated cache for key: {}",
                                                                                                         cacheKey);
                                                                                         logger.info("Successfully trashed role with id: {}",
                                                                                                         id);
                                                                                         return ApiResponse.success(
                                                                                                         "Role trashed successfully",
                                                                                                         roleResponseDeleteAt);
                                                                                 });
                                                         });
                                }).onFailure().invoke(e -> {
                                        logger.error("Error trashing role with id: {}", id, e);
                                });
        }

        @Override
        public Uni<ApiResponse<RoleResponseDeleteAt>> restore(String id) {
                logger.info("Restoring role with id: {}", id);

                Attributes attrs = Attributes.builder()
                                .put("operation", "restore_role")
                                .put("service.name", "role-service")
                                .put("role.id", String.valueOf(id))
                                .build();

                return tracingMetrics.<ApiResponse<RoleResponseDeleteAt>>traceAndMeasure(
                                "restoreRole", "restore_role", attrs, () -> {

                                        return roleRepository.restore(id)
                                                        .chain(restoredRole -> {
                                                                 if (restoredRole == null) {
                                                                         logger.warn(
                                                                                         "Role restore failed - role must be trashed first: {}",
                                                                                         id);
                                                                         throw new InvalidRequestException(
                                                                                         "Role with id " + id
                                                                                                         + " must be trashed first");
                                                                 }

                                                                 RoleResponseDeleteAt roleResponseDeleteAt = RoleResponseDeleteAt
                                                                                 .from(restoredRole);
                                                                 String cacheKey = "role:" + id;

                                                                 return redisService.deleteReactive(cacheKey)
                                                                                 .map(v -> {
                                                                                         logger.info("Invalidated cache for key: {}",
                                                                                                         cacheKey);
                                                                                         logger.info("Successfully restored role with id: {}",
                                                                                                         id);
                                                                                         return ApiResponse.success(
                                                                                                         "Role restored successfully",
                                                                                                         roleResponseDeleteAt);
                                                                                 });
                                                         });
                                }).onFailure().invoke(e -> {
                                        logger.error("Error restoring role with id: {}", id, e);
                                });
        }

        @Override
        public Uni<ApiResponse<Void>> deletePermanent(String id) {
                logger.info("Permanently deleting role with id: {}", id);

                Attributes attrs = Attributes.builder()
                                .put("operation", "delete_role_permanent")
                                .put("service.name", "role-service")
                                .put("role.id", String.valueOf(id))
                                .build();

                return tracingMetrics.<ApiResponse<Void>>traceAndMeasure(
                                "deleteRolePermanent", "delete_role_permanent", attrs, () -> {

                                        return roleRepository.deletePermanent(id)
                                                        .chain(deletedRole -> {
                                                                 if (deletedRole == null) {
                                                                         logger.warn(
                                                                                         "Permanent delete failed - role not found or not trashed: {}",
                                                                                         id);
                                                                         throw new InvalidRequestException(
                                                                                         "Role with id " + id
                                                                                                         + " must be trashed before permanent delete");
                                                                 }

                                                                 String cacheKey = "role:" + id;

                                                                 return redisService.deleteReactive(cacheKey)
                                                                                 .map(v -> {
                                                                                         logger.info("Invalidated cache for key: {}",
                                                                                                         cacheKey);
                                                                                         logger.info("Successfully permanently deleted role with id: {}",
                                                                                                         id);
                                                                                         return ApiResponse.success(
                                                                                                         "Role deleted permanently");
                                                                                 });
                                                         });
                                }).onFailure().invoke(e -> {
                                        logger.error("Error permanently deleting role with id: {}", id, e);
                                });
        }

        @Override
        public Uni<ApiResponse<Void>> restoreAllTrashedRoles() {
                logger.info("Restoring all trashed roles");

                Attributes attrs = Attributes.builder()
                                .put("operation", "restore_all_trashed_roles")
                                .put("service.name", "role-service")
                                .build();

                return tracingMetrics.<ApiResponse<Void>>traceAndMeasure(
                                "restoreAllTrashedRoles", "restore_all_trashed_roles", attrs, () -> {

                                        return roleRepository.restoreAllDeleted()
                                                        .map(restored -> {
                                                                 if (!restored) {
                                                                         throw new ResourceNotFoundException(
                                                                                         "No trashed roles found");
                                                                 }
                                                                 logger.warn("All trashed roles restored. Caches will be refreshed upon expiry or next access.");
                                                                 logger.info("Successfully restored all trashed roles");
                                                                 return ApiResponse.success(
                                                                                 "All trashed roles have been restored successfully");
                                                         });
                                }).onFailure().invoke(e -> {
                                        logger.error("Error restoring all trashed roles", e);
                                });
        }

        @Override
        public Uni<ApiResponse<Void>> deleteAllTrashedRoles() {
                logger.info("Permanently deleting all trashed roles");

                Attributes attrs = Attributes.builder()
                                .put("operation", "delete_all_trashed_roles")
                                .put("service.name", "role-service")
                                .build();

                return tracingMetrics.<ApiResponse<Void>>traceAndMeasure(
                                "deleteAllTrashedRoles", "delete_all_trashed_roles", attrs, () -> {

                                        return roleRepository.deleteAllDeleted()
                                                        .map(deleted -> {
                                                                 if (!deleted) {
                                                                         throw new ResourceNotFoundException(
                                                                                         "No trashed roles found");
                                                                 }
                                                                 logger.warn("All trashed roles deleted. Caches will be refreshed upon expiry or next access.");
                                                                 logger.info("Successfully deleted all trashed roles");
                                                                 return ApiResponse.success(
                                                                                 "All trashed roles have been deleted permanently");
                                                         });
                                }).onFailure().invoke(e -> {
                                        logger.error("Error deleting all trashed roles", e);
                                });
        }

        @Override
        public Uni<ApiResponse<UserRoleResponse>> assignRoleToUser(String userId, String roleId) {
                logger.info("Assigning role with id: {} to user with id: {}", roleId, userId);

                Attributes attrs = Attributes.builder()
                                .put("operation", "assign_role_to_user")
                                .put("service.name", "role-service")
                                .put("user.id", String.valueOf(userId))
                                .put("role.id", String.valueOf(roleId))
                                .build();

                return tracingMetrics.<ApiResponse<UserRoleResponse>>traceAndMeasure(
                                "assignRoleToUser", "assign_role_to_user", attrs, () -> {

                                        return userRoleRepository.assignRole(userId, roleId)
                                                        .map(userRole -> {
                                                                 logger.info("Successfully assigned role with id: {} to user with id: {}",
                                                                                 roleId, userId);
                                                                 return ApiResponse.success("Role assigned to user successfully",
                                                                                 UserRoleResponse.from(userRole));
                                                         });
                                }).onFailure().invoke(e -> {
                                        logger.error("Error assigning role with id: {} to user with id: {}", roleId,
                                                        userId, e);
                                });
        }

        @Override
        public Uni<ApiResponse<Void>> removeRoleFromUser(String userId, String roleId) {
                logger.info("Removing role with id: {} from user with id: {}", roleId, userId);

                Attributes attrs = Attributes.builder()
                                .put("operation", "remove_role_from_user")
                                .put("service.name", "role-service")
                                .put("user.id", String.valueOf(userId))
                                .put("role.id", String.valueOf(roleId))
                                .build();

                return tracingMetrics.<ApiResponse<Void>>traceAndMeasure(
                                "removeRoleFromUser", "remove_role_from_user", attrs, () -> {

                                        return userRoleRepository.removeRole(userId, roleId)
                                                        .map(removed -> {
                                                                 if (!removed) {
                                                                         throw new ResourceNotFoundException(
                                                                                         "UserRole association not found");
                                                                 }
                                                                 logger.info("Successfully removed role with id: {} from user with id: {}",
                                                                                 roleId, userId);
                                                                 return ApiResponse.<Void>success(
                                                                                 "Role removed from user successfully");
                                                         });
                                }).onFailure().invoke(e -> {
                                        logger.error("Error removing role with id: {} from user with id: {}", roleId,
                                                        userId, e);
                                });
        }
}